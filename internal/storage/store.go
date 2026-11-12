package storage

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	_ "github.com/mattn/go-sqlite3"
)

//go:embed migrations/*.sql
var migrations embed.FS

const SchemaVersion = 3
const QueryTimeout = 2 * time.Second

type Store struct {
	write, read     *sql.DB
	dir             string
	budget          int64
	retention       int
	analysisQueries chan struct{}
}

func Open(dir string, budget int64, retention int) (*Store, error) {
	if retention < 1 || retention > 7 {
		return nil, errors.New("raw retention must be 1–7 days")
	}
	path := filepath.Join(dir, "observer.sqlite")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}
	f.Close()
	dsn := (&url.URL{Scheme: "file", Path: path}).String()
	w, err := sql.Open("sqlite3", dsn+"?_busy_timeout=250&_foreign_keys=on&_journal_mode=WAL&_synchronous=FULL&_cache_size=-4096")
	if err != nil {
		return nil, err
	}
	w.SetMaxOpenConns(1)
	s := &Store{write: w, dir: dir, budget: budget, retention: retention, analysisQueries: make(chan struct{}, 1)}
	success := false
	defer func() {
		if !success {
			s.Close()
		}
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var version int
	if err = w.QueryRowContext(ctx, "PRAGMA user_version").Scan(&version); err != nil {
		return nil, err
	}
	if version > SchemaVersion {
		return nil, errors.New("database schema is newer than this release; restore a compatible backup")
	}
	if version == 0 {
		if _, err = w.ExecContext(ctx, "PRAGMA auto_vacuum=INCREMENTAL"); err != nil {
			return nil, err
		}
	}
	cancel()
	for next := version + 1; next <= SchemaVersion; next++ {
		name := map[int]string{1: "001_initial.sql", 2: "002_associations.sql", 3: "003_association_corrections.sql"}[next]
		if err = applyMigration(w, name); err != nil {
			return nil, err
		}
	}
	ctx, postCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer postCancel()
	// Reserve at least 25% of the total budget for WAL, identity, state, and backups.
	var pageSize int64
	if err = w.QueryRowContext(ctx, "PRAGMA page_size").Scan(&pageSize); err != nil {
		return nil, err
	}
	for _, statement := range []string{
		fmt.Sprintf("PRAGMA max_page_count=%d", budget*3/4/pageSize),
		"PRAGMA journal_size_limit=16777216", "PRAGMA wal_autocheckpoint=256", "PRAGMA temp_store=MEMORY",
	} {
		if _, err = w.ExecContext(ctx, statement); err != nil {
			return nil, err
		}
	}
	// A crash leaves an uncertain interval, never an apparently live old session.
	_, err = w.ExecContext(ctx, `UPDATE sessions SET ended_at=(SELECT last_seen FROM runs WHERE runs.boot_id=sessions.boot_id),reason='unclean_shutdown' WHERE ended_at IS NULL;
 UPDATE runs SET stopped_at=last_seen,state='interrupted',unclean=1 WHERE stopped_at IS NULL;`)
	if err != nil {
		return nil, err
	}
	s.read, err = sql.Open("sqlite3", dsn+"?mode=ro&_busy_timeout=250&_query_only=on&_cache_size=-2048")
	if err != nil {
		return nil, err
	}
	s.read.SetMaxOpenConns(4)
	if err = s.read.PingContext(ctx); err != nil {
		return nil, err
	}
	success = true
	return s, nil
}

// Each migration has a separate deadline from connection/startup checks.
// Transactions roll back on interruption; no partly migrated schema is committed.
func applyMigration(db *sql.DB, name string) error {
	b, err := migrations.ReadFile("migrations/" + name)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, string(b)); err != nil {
		return fmt.Errorf("migration %s: %w", name, err)
	}
	return tx.Commit()
}

func (s *Store) Close() {
	if s.read != nil {
		s.read.Close()
	}
	if s.write != nil {
		s.write.Close()
	}
}
func (s *Store) Ping(ctx context.Context) error { return s.read.PingContext(ctx) }
func (s *Store) Checkpoint(ctx context.Context) error {
	_, e := s.write.ExecContext(ctx, "PRAGMA wal_checkpoint(TRUNCATE)")
	return e
}

type event struct {
	Type        string          `json:"type"`
	ObserverID  string          `json:"observerId"`
	BootID      string          `json:"bootId"`
	Started     time.Time       `json:"startedAt"`
	Stopped     time.Time       `json:"stoppedAt"`
	At          time.Time       `json:"at"`
	Release     string          `json:"release"`
	Revision    string          `json:"upstreamRevision"`
	Fingerprint string          `json:"configurationFingerprint"`
	State       string          `json:"state"`
	Drops       uint64          `json:"drops"`
	ClockJumps  int             `json:"clockJumps"`
	Session     capture.Session `json:"session"`
	Header      protocol.Header `json:"header"`
	SessionID   string          `json:"sessionId"`
	Hash        string          `json:"hash"`
	Kind        string          `json:"eventKind"`
	Duplicates  int             `json:"duplicateCount"`
	Name        string          `json:"name"`
	Producer    string          `json:"producerAddress"`
	Source      string          `json:"source"`
	Height      uint64          `json:"height"`
	AgreedHash  string          `json:"agreedHash"`
	Disputed    bool            `json:"disputed"`
}

func millis(t time.Time) int64 { return t.UnixMilli() }
func optionalTime(t *time.Time) any {
	if t == nil {
		return nil
	}
	return millis(*t)
}

func putHeader(ctx context.Context, tx *sql.Tx, h protocol.Header, at time.Time) error {
	if h.Hash.IsZero() {
		return errors.New("empty checked header")
	}
	_, e := tx.ExecContext(ctx, `INSERT INTO momentums(hash,height,previous_hash,timestamp,producer,protocol_version,verification,source,first_seen,last_seen)
 VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(hash) DO UPDATE SET last_seen=max(last_seen,excluded.last_seen),
 signer_conflict=CASE WHEN producer<>excluded.producer THEN 1 ELSE signer_conflict END,
 verification=CASE WHEN producer<>excluded.producer OR signer_conflict=1 THEN 'disputed' WHEN verification IN ('rpc_agreed','disputed') THEN verification WHEN excluded.verification='rpc_observed' THEN 'rpc_observed' ELSE verification END`,
		h.Hash.String(), h.Height, h.PreviousHash.String(), h.Timestamp, h.Producer, h.Version, h.Verification, h.Source, millis(at), millis(at))
	return e
}

// WriteBatch is invoked by the single bounded capture writer. One transaction
// commits all evidence or none. Socket goroutines never wait on this function.
func (s *Store) WriteBatch(batch [][]byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	tx, err := s.write.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, b := range batch {
		var v event
		if err = json.Unmarshal(b, &v); err != nil {
			return err
		}
		switch v.Type {
		case "run_started":
			var owner string
			e := tx.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key='observer_id'").Scan(&owner)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return e
			}
			if e == nil && owner != v.ObserverID {
				return errors.New("OBSERVER_ID differs from existing database")
			}
			_, err = tx.ExecContext(ctx, `INSERT OR IGNORE INTO metadata(key,value) VALUES('observer_id',?);
    INSERT INTO runs(boot_id,observer_id,started_at,last_seen,release,upstream_revision,fingerprint,state) VALUES(?,?,?,?,?,?,?,'initializing')`, v.ObserverID, v.BootID, v.ObserverID, millis(v.Started), millis(v.Started), v.Release, v.Revision, v.Fingerprint)
		case "run_status":
			_, err = tx.ExecContext(ctx, "UPDATE runs SET last_seen=?,state=?,drops=?,clock_jumps=? WHERE boot_id=?", millis(v.At), v.State, v.Drops, v.ClockJumps, v.BootID)
		case "run_stopped":
			_, err = tx.ExecContext(ctx, "UPDATE runs SET stopped_at=?,last_seen=?,state=? WHERE boot_id=?", millis(v.Stopped), millis(v.Stopped), v.State, v.BootID)
		case "session_started", "session_ended":
			p := v.Session
			caps, _ := json.Marshal(p.Capabilities)
			_, err = tx.ExecContext(ctx, `INSERT INTO sessions(session_id,boot_id,peer_id,ip,port,client_name,advertised_version,capabilities,started_at,ready_at,start_mono,ended_at,end_mono,reason)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(session_id) DO UPDATE SET ended_at=excluded.ended_at,end_mono=excluded.end_mono,reason=excluded.reason`,
				p.ID, v.BootID, p.PeerID, p.IP, p.Port, p.ClientName, p.Version, string(caps), millis(p.Started), millis(p.Ready), p.StartMono, optionalTime(p.Ended), p.EndMono, p.Reason)
		case "arrival", "arrival_resolved":
			var a capture.Arrival
			if err = json.Unmarshal(b, &a); err != nil {
				return err
			}
			if a.Header != nil {
				if err = putHeader(ctx, tx, *a.Header, a.UTC); err != nil {
					return err
				}
			}
			// Resolution enriches eligibility but preserves the original arrival clock.
			_, err = tx.ExecContext(ctx, `INSERT INTO arrivals(observer_id,boot_id,session_id,peer_id,hash,kind,arrived_at,arrived_utc,arrived_ns,monotonic_ns,duplicates,eligible,exclusion)
    VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(boot_id,session_id,hash,kind) DO UPDATE SET
    duplicates=max(duplicates,excluded.duplicates),
    eligible=CASE WHEN ?='arrival_resolved' AND arrived_at=excluded.arrived_at AND monotonic_ns=excluded.monotonic_ns THEN excluded.eligible ELSE eligible END,
    exclusion=CASE WHEN ?='arrival_resolved' AND arrived_at=excluded.arrived_at AND monotonic_ns=excluded.monotonic_ns THEN excluded.exclusion ELSE exclusion END`,
				a.ObserverID, a.BootID, a.SessionID, a.PeerID, a.Hash.String(), a.Kind, millis(a.UTC), a.UTC.Format(time.RFC3339Nano), a.UTC.UnixNano(), a.Mono, a.Duplicates, a.Eligible, a.Exclusion, v.Type, v.Type)
		case "duplicate_summary":
			_, err = tx.ExecContext(ctx, "UPDATE arrivals SET duplicates=max(duplicates,?) WHERE session_id=? AND hash=? AND kind=?", v.Duplicates, v.SessionID, v.Hash, v.Kind)
		case "header":
			err = putHeader(ctx, tx, v.Header, v.At)
		case "reference_checked":
			if err = putHeader(ctx, tx, v.Header, v.At); err != nil {
				return err
			}
			_, err = tx.ExecContext(ctx, `INSERT INTO reference_checks(height,source,hash,observed_at) VALUES(?,?,?,?) ON CONFLICT(height,source) DO UPDATE SET hash=excluded.hash,observed_at=excluded.observed_at`, v.Header.Height, v.Source, v.Header.Hash.String(), millis(v.At))
		case "chain_agreement":
			// Competing hashes are retained. Reconciliation can repair a past dispute,
			// but only the agreed hash regains rpc_agreed; other hashes stay disputed.
			if v.Disputed {
				_, err = tx.ExecContext(ctx, "UPDATE momentums SET verification='disputed',reference_checked_at=? WHERE height=?", millis(v.At), v.Height)
			} else {
				_, err = tx.ExecContext(ctx, "UPDATE momentums SET verification=CASE WHEN hash=? AND signer_conflict=0 THEN 'rpc_agreed' ELSE 'disputed' END,reference_checked_at=? WHERE height=?", v.AgreedHash, millis(v.At), v.Height)
			}
		case "pillar_snapshot":
			_, err = tx.ExecContext(ctx, "INSERT INTO pillar_snapshots(producer,name,source,observed_at) VALUES(?,?,?,?)", v.Producer, v.Name, v.Source, millis(v.At))
		case "chain_continuity_lost":
			_, err = tx.ExecContext(ctx, "INSERT OR REPLACE INTO metadata(key,value) VALUES('continuity_unresolved','1')")
		case "clock_discontinuity", "capture_gap", "chain_discontinuity", "connection_attempt_ended", "candidate_rejected":
			if v.At.IsZero() {
				v.At = time.Now()
			}
			_, err = tx.ExecContext(ctx, "INSERT INTO gaps(boot_id,kind,observed_at,detail) VALUES(?,?,?,?)", v.BootID, v.Type, millis(v.At), string(b))
		default:
			return fmt.Errorf("unknown storage event %q", v.Type)
		}
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Maintenance deletes only expired detail in small transactions. Identity,
// traffic state and legacy feasibility evidence are never retention targets.
func (s *Store) Maintenance(ctx context.Context, now time.Time) error {
	cutoff := now.Add(-time.Duration(s.retention) * 24 * time.Hour).UnixMilli()
	statements := []struct {
		q    string
		args []any
	}{
		{"DELETE FROM association_parts WHERE rowid IN (SELECT rowid FROM association_parts WHERE hour<? LIMIT 1000)", []any{now.Add(-90 * 24 * time.Hour).Unix()}},
		{"DELETE FROM association_hours WHERE hour IN (SELECT hour FROM association_hours WHERE hour<? AND NOT EXISTS(SELECT 1 FROM association_parts WHERE association_parts.hour=association_hours.hour) LIMIT 24)", []any{now.Add(-90 * 24 * time.Hour).Unix()}},
		{"DELETE FROM arrivals WHERE id IN (SELECT id FROM arrivals WHERE arrived_at<? ORDER BY arrived_at LIMIT 1000)", []any{cutoff}},
		{"DELETE FROM sessions WHERE session_id IN (SELECT session_id FROM sessions WHERE ended_at<? AND NOT EXISTS(SELECT 1 FROM arrivals WHERE arrivals.session_id=sessions.session_id) AND NOT EXISTS(SELECT 1 FROM association_session_refs WHERE association_session_refs.session_id=sessions.session_id) LIMIT 1000)", []any{cutoff}},
		{"DELETE FROM momentums WHERE hash IN (SELECT hash FROM momentums WHERE last_seen<? AND NOT EXISTS(SELECT 1 FROM arrivals WHERE arrivals.hash=momentums.hash) AND NOT EXISTS(SELECT 1 FROM association_evidence WHERE association_evidence.hash=momentums.hash) LIMIT 1000)", []any{cutoff}},
		{"DELETE FROM pillar_snapshots WHERE id IN (SELECT id FROM pillar_snapshots WHERE observed_at<? LIMIT 1000)", []any{cutoff}},
		{"DELETE FROM gaps WHERE id IN (SELECT id FROM gaps WHERE observed_at<? LIMIT 1000)", []any{cutoff}},
		{"DELETE FROM reference_checks WHERE rowid IN (SELECT rowid FROM reference_checks WHERE observed_at<? LIMIT 1000)", []any{cutoff}},
		{"DELETE FROM runs WHERE boot_id IN (SELECT boot_id FROM runs WHERE stopped_at<? AND NOT EXISTS(SELECT 1 FROM sessions WHERE sessions.boot_id=runs.boot_id) LIMIT 100)", []any{cutoff}},
	}
	for _, v := range statements {
		if _, e := s.write.ExecContext(ctx, v.q, v.args...); e != nil {
			return e
		}
	}
	if _, err := s.write.ExecContext(ctx, "PRAGMA incremental_vacuum(128)"); err != nil {
		return err
	}
	_, err := s.write.ExecContext(ctx, "PRAGMA wal_checkpoint(PASSIVE)")
	return err
}
