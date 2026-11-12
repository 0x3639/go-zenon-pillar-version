package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/analysis"
)

// AnalyzeNext replays a single complete hour in a read snapshot, then atomically
// replaces its summaries only if the source revision has not changed. A two-minute
// grace permits reference reconciliation. Late resolution/reorgs dirty the hour.
func (s *Store) AnalyzeNext(ctx context.Context, now time.Time) (bool, error) {
	if s.ContinuityUnresolved(ctx) {
		return false, errors.New("chain continuity unresolved")
	}
	var hour, revision int64
	err := s.read.QueryRowContext(ctx, `SELECT hour,revision FROM association_hours WHERE revision<>computed_revision AND hour+3600<=? ORDER BY hour LIMIT 1`, now.Add(-2*time.Minute).Unix()).Scan(&hour, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	start := time.Unix(hour, 0).UTC()
	end := start.Add(time.Hour)
	parts := []analysis.Part{}
	exclusions := map[string]int{}
	if start.Before(now.Add(-time.Duration(s.retention) * 24 * time.Hour)) {
		// Never rebuild a partially expired hour into an apparently complete result.
		exclusions["raw_history_expired"] = 1
	} else {
		parts, exclusions, err = s.analyzeHour(ctx, start, end)
		if err != nil {
			return false, err
		}
	}
	if len(parts) > 4096 {
		return false, errors.New("analysis part-per-hour bound exceeded")
	}
	writeCtx, stopWrite := context.WithTimeout(ctx, 500*time.Millisecond)
	defer stopWrite()
	tx, err := s.write.BeginTx(writeCtx, nil)
	if err != nil {
		return false, err
	}
	defer tx.Rollback()
	var current int64
	if err = tx.QueryRowContext(writeCtx, "SELECT revision FROM association_hours WHERE hour=?", hour).Scan(&current); err != nil {
		return false, err
	}
	if current != revision {
		return true, nil
	}
	if _, err = tx.ExecContext(writeCtx, "DELETE FROM association_parts WHERE hour=?", hour); err != nil {
		return false, err
	}
	for _, p := range parts {
		b, e := json.Marshal(p)
		if e != nil {
			return false, e
		}
		if _, err = tx.ExecContext(writeCtx, "INSERT INTO association_parts(hour,cohort,producer,peer,kind,boot_id,payload) VALUES(?,?,?,?,?,?,?)", hour, p.Cohort, p.Producer, p.Peer, p.Kind, p.Boot, string(b)); err != nil {
			return false, err
		}
	}
	if _, err = tx.ExecContext(writeCtx, `INSERT OR IGNORE INTO association_evidence(hour,hash) SELECT ?,hash FROM arrivals WHERE arrived_at>=? AND arrived_at<? AND kind IN ('hash_announcement','full_broadcast')`, hour, start.UnixMilli(), end.Add(10*time.Second).UnixMilli()); err != nil {
		return false, err
	}
	b, _ := json.Marshal(exclusions)
	if _, err = tx.ExecContext(writeCtx, "UPDATE association_hours SET computed_revision=?,computed_at=?,exclusions=? WHERE hour=?", revision, now.UnixMilli(), string(b), hour); err != nil {
		return false, err
	}
	return true, tx.Commit()
}

func (s *Store) analyzeHour(ctx context.Context, start, end time.Time) ([]analysis.Part, map[string]int, error) {
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return nil, nil, err
	}
	defer tx.Rollback()
	runs := []analysis.Run{}
	sessions := []analysis.Session{}
	arrivals := []analysis.Arrival{}
	rows, err := tx.QueryContext(ctx, `SELECT boot_id,last_seen,drops,clock_jumps,unclean FROM runs WHERE started_at<? AND (stopped_at IS NULL OR stopped_at>=?) LIMIT 129`, end.Add(10*time.Second).UnixMilli(), start.Add(-130*time.Second).UnixMilli())
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var r analysis.Run
		var seen int64
		var d, c, u int
		if err = rows.Scan(&r.Boot, &seen, &d, &c, &u); err != nil {
			break
		}
		r.LastSeen = time.UnixMilli(seen)
		r.Unsafe = d > 0 || c > 0 || u > 0
		runs = append(runs, r)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	if len(runs) > 128 {
		return nil, nil, errors.New("analysis run bound exceeded")
	}
	rows, err = tx.QueryContext(ctx, `SELECT session_id,boot_id,peer_id,ip,advertised_version,ready_at,start_mono,ended_at,end_mono FROM sessions WHERE ready_at<? AND (ended_at IS NULL OR ended_at>=?) LIMIT 2049`, end.Add(10*time.Second).UnixMilli(), start.UnixMilli())
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var v analysis.Session
		var ready int64
		var ended, mono sql.NullInt64
		if err = rows.Scan(&v.ID, &v.Boot, &v.Peer, &v.IP, &v.Version, &ready, &v.StartMono, &ended, &mono); err != nil {
			break
		}
		v.Ready = time.UnixMilli(ready).UTC()
		v.Ended = ended.Valid
		v.EndMono = mono.Int64
		sessions = append(sessions, v)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	if len(sessions) > 2048 {
		return nil, nil, errors.New("analysis session bound exceeded")
	}
	// The correlated minimum prevents a repeated/late delivery crossing an hour
	// boundary from becoming a new comparison. Requested responses are never inputs.
	rows, err = tx.QueryContext(ctx, `SELECT a.boot_id,a.session_id,a.hash,a.kind,a.arrived_ns,a.monotonic_ns,a.eligible,
 coalesce(m.producer,''),coalesce(m.verification,'unresolved'),coalesce(m.signer_conflict,0)
 FROM arrivals a LEFT JOIN momentums m ON m.hash=a.hash
 WHERE a.arrived_at>=? AND a.arrived_at<? AND a.kind IN ('hash_announcement','full_broadcast')
 AND (SELECT min(b.arrived_ns) FROM arrivals b WHERE b.hash=a.hash AND b.boot_id=a.boot_id AND b.kind=a.kind)>=?
 ORDER BY a.arrived_at,a.id LIMIT 20001`, start.UnixMilli(), end.Add(10*time.Second).UnixMilli(), start.UnixNano())
	if err != nil {
		return nil, nil, err
	}
	for rows.Next() {
		var a analysis.Arrival
		var ns int64
		if err = rows.Scan(&a.Boot, &a.Session, &a.Hash, &a.Kind, &ns, &a.Mono, &a.Eligible, &a.Producer, &a.Verification, &a.SignerConflict); err != nil {
			break
		}
		a.At = time.Unix(0, ns).UTC()
		arrivals = append(arrivals, a)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return nil, nil, err
	}
	if len(arrivals) > 20000 {
		return nil, nil, errors.New("analysis arrival bound exceeded")
	}
	parts, excluded, computeErr := analysis.ComputeContext(ctx, start, end, runs, sessions, arrivals)
	if computeErr != nil {
		return nil, nil, computeErr
	}
	// Names are explicitly dated snapshots, never a retrospective assignment proof.
	names := map[string]struct {
		name string
		at   time.Time
	}{}
	for i := range parts {
		p := &parts[i]
		n, ok := names[p.Producer]
		if !ok {
			var name string
			var at int64
			e := tx.QueryRowContext(ctx, "SELECT name,observed_at FROM pillar_snapshots WHERE producer=? AND observed_at<? ORDER BY observed_at DESC LIMIT 1", p.Producer, end.UnixMilli()).Scan(&name, &at)
			if e != nil && !errors.Is(e, sql.ErrNoRows) {
				return nil, nil, e
			}
			if e == nil {
				n.name = name
				n.at = time.UnixMilli(at).UTC()
			}
			names[p.Producer] = n
		}
		if n.name != "" {
			name, at := n.name, n.at
			p.Name = &name
			p.NameAt = &at
		}
	}
	return parts, excluded, tx.Commit()
}

type AssociationPage struct {
	Data            []analysis.Row `json:"data"`
	NextOffset      *int           `json:"nextOffset"`
	Available       bool           `json:"available"`
	Reason          string         `json:"reason,omitempty"`
	ProducerVersion *string        `json:"producerSoftwareVersion"`
	ProcessedHours  int            `json:"processedHours"`
	PendingHours    int            `json:"pendingHours"`
	MissingHours    int            `json:"missingHours"`
	Exclusions      map[string]int `json:"excludedComparisonsByReason"`
	ComputedAt      *time.Time     `json:"computedAt"`
	Method          string         `json:"methodVersion"`
}

func (s *Store) Associations(ctx context.Context, p Query) (AssociationPage, error) {
	out := AssociationPage{Data: []analysis.Row{}, Available: true, Exclusions: map[string]int{}, Method: analysis.Method}
	if p.Limit < 1 || p.Limit > 500 || p.Offset < 0 || p.Offset > 100000 || !p.End.After(p.Start) || p.End.Sub(p.Start) > 90*24*time.Hour || !p.Start.Equal(p.Start.Truncate(time.Hour)) || !p.End.Equal(p.End.Truncate(time.Hour)) {
		return out, errors.New("invalid association query bounds")
	}
	select {
	case s.analysisQueries <- struct{}{}:
		defer func() { <-s.analysisQueries }()
	default:
		return out, errors.New("analysis query capacity")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	tx, err := s.read.BeginTx(ctx, &sql.TxOptions{ReadOnly: true})
	if err != nil {
		return out, err
	}
	defer tx.Rollback()
	var continuity string
	if err = tx.QueryRowContext(ctx, "SELECT coalesce((SELECT value FROM metadata WHERE key='continuity_unresolved'),'0')").Scan(&continuity); err != nil {
		return out, err
	}
	if continuity == "1" {
		out.Available = false
		out.Reason = "chain_continuity_unresolved"
		return out, nil
	}

	rows, err := tx.QueryContext(ctx, "SELECT hour,revision,computed_revision,computed_at,exclusions FROM association_hours WHERE hour>=? AND hour<? ORDER BY hour", p.Start.Unix(), p.End.Unix())
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var h, rev, done int64
		var at sql.NullInt64
		var raw string
		if err = rows.Scan(&h, &rev, &done, &at, &raw); err != nil {
			break
		}
		if rev != done {
			out.PendingHours++
			continue
		}
		out.ProcessedHours++
		if at.Valid {
			t := time.UnixMilli(at.Int64).UTC()
			if out.ComputedAt == nil || t.After(*out.ComputedAt) {
				out.ComputedAt = &t
			}
		}
		var reasons map[string]int
		if err = json.Unmarshal([]byte(raw), &reasons); err != nil {
			break
		}
		for k, v := range reasons {
			out.Exclusions[k] += v
		}
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return out, err
	}
	out.MissingHours = int(p.End.Sub(p.Start)/time.Hour) - out.ProcessedHours - out.PendingHours
	if out.ProcessedHours == 0 {
		out.Available = false
		out.Reason = "analysis_pending_or_no_observations"
	}
	// Page row identities before reading statistics. Baselines were computed from
	// all producers at build time, so filtering cannot change their meaning.
	filter := "p.hour>=? AND p.hour<? AND h.revision=h.computed_revision"
	args := []any{p.Start.Unix(), p.End.Unix()}
	for _, f := range []struct{ column, value string }{{"producer", p.Producer}, {"peer", p.Peer}, {"kind", p.Kind}} {
		if f.value != "" {
			filter += " AND p." + f.column + "=?"
			args = append(args, f.value)
		}
	}
	keyQuery := `SELECT DISTINCT p.cohort,p.producer,p.peer,p.kind FROM association_parts p JOIN association_hours h ON h.hour=p.hour WHERE ` + filter + ` ORDER BY p.producer,p.peer,p.cohort,p.kind LIMIT ? OFFSET ?`
	keyArgs := append(append([]any{}, args...), p.Limit+1, p.Offset)
	rows, err = tx.QueryContext(ctx, keyQuery, keyArgs...)
	if err != nil {
		return out, err
	}
	keys := 0
	for rows.Next() {
		keys++
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return out, err
	}
	if keys > p.Limit {
		n := p.Offset + p.Limit
		if n <= 100000 {
			out.NextOffset = &n
		}
	}
	report := analysis.NewReport(p.Start, p.End)
	selectedArgs := append(append([]any{}, args...), p.Limit, p.Offset)
	selectedArgs = append(selectedArgs, p.Start.Unix(), p.End.Unix())
	rows, err = tx.QueryContext(ctx, `WITH selected AS (`+keyQuery+`)
 SELECT p.hour,p.payload FROM association_parts p JOIN selected s ON p.cohort=s.cohort AND p.producer=s.producer AND p.peer=s.peer AND p.kind=s.kind JOIN association_hours h ON h.hour=p.hour
 WHERE p.hour>=? AND p.hour<? AND h.revision=h.computed_revision ORDER BY p.hour,p.cohort,p.producer,p.peer,p.kind LIMIT 200001`, selectedArgs...)
	if err != nil {
		return out, err
	}
	batch := []analysis.Part{}
	var current int64
	count := 0
	delaySamples := 0
	for rows.Next() {
		var h int64
		var raw []byte
		if err = rows.Scan(&h, &raw); err != nil {
			break
		}
		count++
		if count > 200000 {
			err = errors.New("analysis part bound exceeded; narrow window")
			break
		}
		if h != current && len(batch) > 0 {
			report.AddHour(batch)
			batch = nil
		}
		current = h
		var part analysis.Part
		if err = json.Unmarshal(raw, &part); err != nil {
			break
		}
		delaySamples += len(part.Delays)
		if delaySamples > 1000000 {
			err = errors.New("analysis delay-sample bound exceeded; narrow window or page")
			break
		}
		batch = append(batch, part)
	}
	if err == nil {
		err = rows.Err()
	}
	rows.Close()
	if err != nil {
		return out, err
	}
	report.AddHour(batch)
	out.Data = report.Finish()
	cutoff := time.Now().UTC().Add(-time.Duration(s.retention) * 24 * time.Hour)
	for i := range out.Data {
		out.Data[i].RawAvailable = !out.Data[i].FirstAt.Truncate(time.Hour).Before(cutoff)
	}
	if ctx.Err() != nil {
		return out, fmt.Errorf("analysis deadline: %w", ctx.Err())
	}
	return out, tx.Commit()
}
