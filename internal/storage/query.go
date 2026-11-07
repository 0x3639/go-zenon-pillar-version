package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

type Query struct {
	Start, End                 time.Time
	Limit, Offset              int
	Producer, Peer, Hash, Kind string
}
type Page struct {
	Data       []json.RawMessage `json:"data"`
	NextOffset *int              `json:"nextOffset"`
}

const stamp = "'%Y-%m-%dT%H:%M:%fZ'"

// Collection returns bounded session/history rows. Relay versions always come
// from the historical session; no query invents a producer software version.
func (s *Store) Collection(ctx context.Context, resource string, p Query) (Page, error) {
	out := Page{Data: []json.RawMessage{}}
	if p.Limit < 1 || p.Limit > 500 || p.Offset < 0 || p.Offset > 100000 || !p.End.After(p.Start) || p.End.Sub(p.Start) > 24*time.Hour {
		return out, errors.New("invalid query bounds")
	}
	ctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	args := []any{}
	var q string
	switch resource {
	case "peers":
		q = `SELECT json_object('sessionId',s.session_id,'bootId',s.boot_id,'peerId',s.peer_id,'ip',s.ip,'port',s.port,
   'clientName',s.client_name,'advertisedVersion',s.advertised_version,'capabilities',json(s.capabilities),
   'versionObservedAt',strftime(` + stamp + `,s.ready_at/1000.0,'unixepoch'),
   'connectedAt',strftime(` + stamp + `,s.started_at/1000.0,'unixepoch'),
   'disconnectedAt',strftime(` + stamp + `,s.ended_at/1000.0,'unixepoch'),
   'disconnectReason',s.reason,'statusCompletedMonoNs',s.start_mono,'disconnectedMonoNs',s.end_mono,
   'uncleanShutdown',json(CASE WHEN r.unclean=1 THEN 'true' ELSE 'false' END))
   FROM sessions s JOIN runs r ON r.boot_id=s.boot_id WHERE s.started_at<? AND (s.ended_at IS NULL OR s.ended_at>=?)`
		args = append(args, p.End.UnixMilli(), p.Start.UnixMilli())
		if p.Peer != "" {
			q += " AND s.peer_id=?"
			args = append(args, p.Peer)
		}
		q += " ORDER BY s.started_at DESC,s.session_id"
	case "momentums":
		q = `SELECT json_object('hash',m.hash,'height',m.height,'previousHash',m.previous_hash,'timestamp',m.timestamp,
   'producerAddress',CASE WHEN m.signer_conflict=0 THEN m.producer ELSE null END,'signerConflict',json(CASE WHEN m.signer_conflict=1 THEN 'true' ELSE 'false' END),'protocolVersion',m.protocol_version,'verificationState',m.verification,
   'source',m.source,'firstObservedAt',strftime(` + stamp + `,m.first_seen/1000.0,'unixepoch'),
   'referenceCheckedAt',strftime(` + stamp + `,m.reference_checked_at/1000.0,'unixepoch'),
   'producerSoftwareVersion',null) FROM momentums m WHERE m.timestamp>=? AND m.timestamp<?`
		args = append(args, ceilUnix(p.Start), ceilUnix(p.End))
		if p.Producer != "" {
			q += " AND m.producer=?"
			args = append(args, p.Producer)
		}
		q += " ORDER BY m.timestamp DESC,m.hash"
	case "arrivals":
		q = `SELECT json_object('observerId',a.observer_id,'bootId',a.boot_id,'sessionId',a.session_id,
   'peerId',a.peer_id,'hash',a.hash,'eventKind',a.kind,'arrivedAt',a.arrived_utc,
   'monotonicNs',a.monotonic_ns,'duplicateCount',a.duplicates,
   'eligible',json(CASE WHEN a.eligible=1 THEN 'true' ELSE 'false' END),'exclusionReason',a.exclusion,
   'verificationState',coalesce(m.verification,'unresolved'),'producerAddress',CASE WHEN m.signer_conflict=0 THEN m.producer ELSE null END,
   'ip',s.ip,'advertisedVersion',s.advertised_version,
   'versionObservedAt',strftime(` + stamp + `,s.ready_at/1000.0,'unixepoch'),
   'producerSoftwareVersion',null,'rankingEligible',json('false'))
   FROM arrivals a JOIN sessions s ON s.session_id=a.session_id LEFT JOIN momentums m ON m.hash=a.hash
   WHERE a.hash=? AND a.arrived_ns>=? AND a.arrived_ns<?`
		args = append(args, p.Hash, p.Start.UnixNano(), p.End.UnixNano())
		if p.Kind != "" {
			q += " AND a.kind=?"
			args = append(args, p.Kind)
		}
		q += " ORDER BY a.arrived_at,a.id"
	case "pillar":
		q = `SELECT json_object('producerAddress',producer,'pillarName',name,'source',source,
   'observedAt',strftime(` + stamp + `,observed_at/1000.0,'unixepoch'),
   'historicalAssignmentVerified',json('false'),'producerSoftwareVersion',null)
   FROM pillar_snapshots WHERE producer=? AND observed_at>=? AND observed_at<? ORDER BY observed_at DESC,id`
		args = append(args, p.Producer, p.Start.UnixMilli(), p.End.UnixMilli())
	default:
		return out, errors.New("unknown collection")
	}
	q += " LIMIT ? OFFSET ?"
	args = append(args, p.Limit+1, p.Offset)
	rows, err := s.read.QueryContext(ctx, q, args...)
	if err != nil {
		return out, err
	}
	defer rows.Close()
	for rows.Next() {
		var b []byte
		if err = rows.Scan(&b); err != nil {
			return out, err
		}
		out.Data = append(out.Data, json.RawMessage(b))
	}
	if err = rows.Err(); err != nil {
		return out, err
	}
	if len(out.Data) > p.Limit {
		out.Data = out.Data[:p.Limit]
		n := p.Offset + p.Limit
		if n <= 100000 {
			out.NextOffset = &n
		}
	}
	return out, nil
}

func ceilUnix(t time.Time) int64 {
	n := t.Unix()
	if t.Nanosecond() != 0 {
		n++
	}
	return n
}

func (s *Store) Quality(ctx context.Context, start, end time.Time) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	var drops, jumps, unclean, gaps int64
	err := s.read.QueryRowContext(ctx, `SELECT coalesce(sum(drops),0),coalesce(sum(clock_jumps),0),coalesce(sum(unclean),0)
  FROM runs WHERE started_at<? AND (stopped_at IS NULL OR stopped_at>=?)`, end.UnixMilli(), start.UnixMilli()).Scan(&drops, &jumps, &unclean)
	if err != nil {
		return nil, err
	}
	err = s.read.QueryRowContext(ctx, "SELECT count(*) FROM gaps WHERE observed_at>=? AND observed_at<?", start.UnixMilli(), end.UnixMilli()).Scan(&gaps)
	return map[string]any{"captureDropsInOverlappingRuns": drops, "clockJumpsInOverlappingRuns": jumps, "interruptedRuns": unclean, "recordedGapAndConnectionEvents": gaps,
		"rankingsAvailable": false, "associationStatus": "insufficient_data", "analysisState": "not_implemented",
		"producerSoftwareVersion": nil, "consensusValidated": false, "absenceMeansOffline": false,
		"rawRetentionDays": s.retention, "rawHistoryAvailableFrom": time.Now().UTC().Truncate(30 * time.Second).Add(-time.Duration(s.retention) * 24 * time.Hour),
		"legacyFeasibilityImported": false}, err
}

func (s *Store) Anchor(ctx context.Context) (map[uint64]string, error) {
	rows, e := s.read.QueryContext(ctx, "SELECT height,hash FROM momentums WHERE verification='rpc_agreed' ORDER BY height DESC LIMIT 180")
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := map[uint64]string{}
	for rows.Next() {
		var h uint64
		var hash string
		if e = rows.Scan(&h, &hash); e != nil {
			return nil, e
		}
		out[h] = hash
	}
	return out, rows.Err()
}

func (s *Store) ContinuityUnresolved(ctx context.Context) bool {
	ctx, cancel := context.WithTimeout(ctx, QueryTimeout)
	defer cancel()
	var value string
	if err := s.read.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key='continuity_unresolved'").Scan(&value); err != nil {
		return !errors.Is(err, sql.ErrNoRows)
	}
	return value == "1"
}
