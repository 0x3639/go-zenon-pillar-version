package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/zenon-network/go-zenon/common/types"
)

func associationFixture(t *testing.T) (*Store, time.Time, capture.Arrival) {
	t.Helper()
	s, e := Open(t.TempDir(), 2<<30, 7)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	start := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	version := "0.0.9"
	if e = s.WriteBatch(records(map[string]any{"type": "run_started", "observerId": "test", "bootId": "boot", "startedAt": start}, map[string]any{"type": "run_status", "bootId": "boot", "at": start.Add(2 * time.Hour)})); e != nil {
		t.Fatal(e)
	}
	for _, id := range []string{"a", "b", "c"} {
		session := capture.Session{ID: id, PeerID: strings.Repeat(id, 128), IP: id, Version: &version, Started: start, Ready: start, StartMono: 0}
		if e = s.WriteBatch(records(map[string]any{"type": "session_started", "bootId": "boot", "session": session})); e != nil {
			t.Fatal(e)
		}
	}
	h := protocol.Header{Hash: types.NewHash([]byte("sample")), Height: 1, Producer: "producer", Timestamp: uint64(start.Add(time.Minute).Unix()), Verification: "signature_checked"}
	a := capture.Arrival{Type: "arrival", ObserverID: "test", BootID: "boot", SessionID: "a", PeerID: strings.Repeat("a", 128), Hash: h.Hash, Kind: "hash_announcement", UTC: start.Add(time.Minute), Mono: int64(time.Minute), Eligible: true, Header: &h}
	b := a
	b.SessionID = "b"
	b.PeerID = strings.Repeat("b", 128)
	b.Mono += int64(time.Second)
	b.UTC = b.UTC.Add(time.Second)
	if e = s.WriteBatch(records(a, b, map[string]any{"type": "chain_agreement", "height": 1, "agreedHash": h.Hash.String(), "at": start.Add(2 * time.Minute)})); e != nil {
		t.Fatal(e)
	}
	return s, start, a
}
func analyzeAll(t *testing.T, s *Store) {
	t.Helper()
	for i := 0; i < 200; i++ {
		ok, e := s.AnalyzeNext(context.Background(), time.Now())
		if e != nil {
			t.Fatal(e)
		}
		if !ok {
			return
		}
	}
	t.Fatal("analysis did not settle")
}
func TestAssociationPersistenceReorgAndExpiry(t *testing.T) {
	s, start, a := associationFixture(t)
	p := Query{Start: start, End: start.Add(time.Hour), Limit: 100}
	analyzeAll(t, s)
	page, e := s.Associations(context.Background(), p)
	if e != nil || len(page.Data) != 3 || page.ProcessedHours != 1 || !page.Available {
		t.Fatal(page, e)
	}
	for _, r := range page.Data {
		if r.Eligible != 1 || r.ProducerVersion != nil || r.Status != "insufficient_data" {
			t.Fatal(r)
		}
		if r.Peer == strings.Repeat("c", 128) && r.Received != 0 {
			t.Fatal(r)
		}
	}
	// Filtering must preserve the baseline and denominator; pagination is stable.
	p.Limit = 1
	page, e = s.Associations(context.Background(), p)
	if e != nil || page.NextOffset == nil || *page.NextOffset != 1 {
		t.Fatal(page, e)
	}
	p.Limit = 100
	if e = s.WriteBatch(records(map[string]any{"type": "chain_agreement", "height": 1, "disputed": true, "at": time.Now()})); e != nil {
		t.Fatal(e)
	}
	page, e = s.Associations(context.Background(), p)
	if e != nil || len(page.Data) != 0 || page.PendingHours != 1 {
		t.Fatal("stale disputed result served", page, e)
	}
	analyzeAll(t, s)
	page, e = s.Associations(context.Background(), p)
	if e != nil || len(page.Data) != 0 || page.Exclusions["unresolved_or_disputed"] != 1 {
		t.Fatal(page, e)
	}
	if e = s.WriteBatch(records(map[string]any{"type": "chain_agreement", "height": 1, "agreedHash": a.Hash.String(), "at": time.Now()})); e != nil {
		t.Fatal(e)
	}
	analyzeAll(t, s)
	// Delete raw records as normal retention would; summaries remain queryable.
	if _, e = s.write.Exec("DELETE FROM arrivals"); e != nil {
		t.Fatal(e)
	}
	page, e = s.Associations(context.Background(), p)
	if e != nil || len(page.Data) != 3 {
		t.Fatal(page, e)
	}
	if e = s.Maintenance(context.Background(), start.Add(91*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	page, e = s.Associations(context.Background(), p)
	if e != nil || len(page.Data) != 0 {
		t.Fatal(page, e)
	}
}
func TestLateBoundaryResolutionInvalidatesAnchorHour(t *testing.T) {
	s, start, a := associationFixture(t)
	a.Hash = types.NewHash([]byte("boundary"))
	a.Header = &protocol.Header{Hash: a.Hash, Height: 2, Producer: "producer", Timestamp: uint64(start.Add(time.Hour - time.Second).Unix()), Verification: "rpc_agreed"}
	a.UTC = start.Add(time.Hour - time.Second)
	a.Mono = int64(time.Hour - time.Second)
	b := a
	b.SessionID = "b"
	b.PeerID = strings.Repeat("b", 128)
	b.UTC = b.UTC.Add(2 * time.Second)
	b.Mono += int64(2 * time.Second)
	b.Eligible = false
	b.Exclusion = "unresolved"
	if e := s.WriteBatch(records(a, b)); e != nil {
		t.Fatal(e)
	}
	analyzeAll(t, s)
	b.Type = "arrival_resolved"
	b.Eligible = true
	b.Exclusion = ""
	if e := s.WriteBatch(records(b)); e != nil {
		t.Fatal(e)
	}
	var revision, computed int
	s.read.QueryRow("SELECT revision,computed_revision FROM association_hours WHERE hour=?", start.Unix()).Scan(&revision, &computed)
	if revision == computed {
		t.Fatal("anchor hour not invalidated")
	}
	analyzeAll(t, s)
	page, e := s.Associations(context.Background(), Query{Start: start, End: start.Add(time.Hour), Limit: 100})
	if e != nil {
		t.Fatal(e)
	}
	found := false
	for _, r := range page.Data {
		if r.Peer == b.PeerID {
			found = true
			if r.Received != 2 {
				t.Fatal(r)
			}
		}
	}
	if !found {
		t.Fatal("resolved peer missing", page)
	}
}
func TestUnsafeRunRevokesSummariesAndCleanRestartPreservesThem(t *testing.T) {
	s, start, _ := associationFixture(t)
	analyzeAll(t, s)
	if _, e := s.write.Exec("UPDATE sessions SET ended_at=?,end_mono=?", start.Add(2*time.Hour).UnixMilli(), int64(2*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if e := s.WriteBatch(records(map[string]any{"type": "run_stopped", "bootId": "boot", "stoppedAt": start.Add(2 * time.Hour)})); e != nil {
		t.Fatal(e)
	}
	// A clean close is required; startup marks open prior runs uncertain.
	s.Close()
	reopened, e := Open(s.dir, 2<<30, 7)
	if e != nil {
		t.Fatal(e)
	}
	defer reopened.Close()
	analyzeAll(t, reopened)
	p := Query{Start: start, End: start.Add(time.Hour), Limit: 100}
	page, e := reopened.Associations(context.Background(), p)
	if e != nil || len(page.Data) != 3 {
		t.Fatal(page, e)
	}
	if e = reopened.WriteBatch(records(map[string]any{"type": "run_status", "bootId": "boot", "at": start.Add(2 * time.Hour), "clockJumps": 1})); e != nil {
		t.Fatal(e)
	}
	page, e = reopened.Associations(context.Background(), p)
	if e != nil || len(page.Data) != 0 {
		t.Fatal(page, e)
	}
	analyzeAll(t, reopened)
	page, e = reopened.Associations(context.Background(), p)
	if e != nil || page.Exclusions["unsafe_or_unknown_run"] != 1 {
		t.Fatal(page, e)
	}
}
func TestMigrationFromSchemaOnePreservesEvidence(t *testing.T) {
	dir := t.TempDir()
	db, e := sql.Open("sqlite3", filepath.Join(dir, "observer.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	initial, e := migrations.ReadFile("migrations/001_initial.sql")
	if e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(string(initial)); e != nil {
		t.Fatal(e)
	}
	if _, e = db.Exec(`INSERT INTO runs(boot_id,observer_id,started_at,last_seen,stopped_at,release,upstream_revision,fingerprint,state) VALUES('old','test',0,1000,1000,'0.2.0','','','stopped'); INSERT INTO sessions(session_id,boot_id,peer_id,ip,port,client_name,capabilities,started_at,ready_at,start_mono,ended_at,end_mono) VALUES('session','old','peer','ip',1,'client','[]',0,0,0,1000,1000000000); INSERT INTO arrivals(observer_id,boot_id,session_id,peer_id,hash,kind,arrived_at,arrived_utc,arrived_ns,monotonic_ns,eligible,exclusion) VALUES('test','old','session','peer','hash','hash_announcement',123,'1970-01-01T00:00:00.123456789Z',123456789,987654321,1,'')`); e != nil {
		t.Fatal(e)
	}
	db.Close()
	s, e := Open(dir, 2<<30, 7)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var utc string
	var ns, mono int64
	var pending, version int
	if e = s.read.QueryRow("SELECT arrived_utc,arrived_ns,monotonic_ns FROM arrivals").Scan(&utc, &ns, &mono); e != nil {
		t.Fatal(e)
	}
	s.read.QueryRow("PRAGMA user_version").Scan(&version)
	s.read.QueryRow("SELECT count(*) FROM association_hours WHERE revision<>computed_revision").Scan(&pending)
	if utc != "1970-01-01T00:00:00.123456789Z" || ns != 123456789 || mono != 987654321 || pending != 1 || version != SchemaVersion {
		t.Fatal(utc, ns, mono, pending, version)
	}
}

func TestExpiredEvidenceIsInvalidatedByDeepReorganization(t *testing.T) {
	s, start, a := associationFixture(t)
	analyzeAll(t, s)
	if _, e := s.write.Exec("DELETE FROM arrivals"); e != nil {
		t.Fatal(e)
	}
	// Supporting headers survive raw pruning and can revoke older summaries.
	if e := s.Maintenance(context.Background(), time.Now().Add(8*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	var n int
	s.read.QueryRow("SELECT count(*) FROM momentums WHERE hash=?", a.Hash.String()).Scan(&n)
	if n != 1 {
		t.Fatal("supporting header removed")
	}
	if e := s.WriteBatch(records(map[string]any{"type": "chain_agreement", "height": 1, "disputed": true, "at": time.Now()})); e != nil {
		t.Fatal(e)
	}
	page, e := s.Associations(context.Background(), Query{Start: start, End: start.Add(time.Hour), Limit: 100})
	if e != nil || len(page.Data) != 0 || page.PendingHours != 1 {
		t.Fatal(page, e)
	}
	worked, e := s.AnalyzeNext(context.Background(), time.Now().Add(8*24*time.Hour))
	if e != nil || !worked {
		t.Fatal(e)
	}
	page, e = s.Associations(context.Background(), Query{Start: start, End: start.Add(time.Hour), Limit: 100})
	if e != nil || page.Exclusions["raw_history_expired"] != 1 {
		t.Fatal(page, e)
	}
}

func TestContinuityLossAndQueryCancellation(t *testing.T) {
	s, start, _ := associationFixture(t)
	analyzeAll(t, s)
	p := Query{Start: start, End: start.Add(time.Hour), Limit: 100}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := s.Associations(ctx, p); e == nil {
		t.Fatal("canceled query succeeded")
	}
	if e := s.WriteBatch(records(map[string]any{"type": "chain_continuity_lost", "bootId": "boot", "at": time.Now()})); e != nil {
		t.Fatal(e)
	}
	page, e := s.Associations(context.Background(), p)
	if e != nil || page.Available || len(page.Data) != 0 || page.Reason != "chain_continuity_unresolved" {
		t.Fatal(page, e)
	}
}

func TestLaterDisconnectDoesNotRevokeExpiredEarlierHours(t *testing.T) {
	s, start, _ := associationFixture(t)
	analyzeAll(t, s)
	if _, e := s.write.Exec("DELETE FROM arrivals"); e != nil {
		t.Fatal(e)
	}
	// A later disconnect cannot alter membership in an earlier completed window.
	if _, e := s.write.Exec("UPDATE sessions SET ended_at=?,end_mono=? WHERE session_id='a'", start.Add(8*24*time.Hour).UnixMilli(), int64(8*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	page, e := s.Associations(context.Background(), Query{Start: start, End: start.Add(time.Hour), Limit: 100})
	if e != nil || len(page.Data) != 3 || page.PendingHours != 0 {
		t.Fatal(page, e)
	}
}
