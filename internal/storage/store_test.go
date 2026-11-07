package storage

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/zenon-network/go-zenon/common/types"
)

func records(v ...any) [][]byte {
	out := [][]byte{}
	for _, x := range v {
		b, _ := json.Marshal(x)
		out = append(out, b)
	}
	return out
}
func fixture(t *testing.T) (*Store, string, capture.Arrival) {
	t.Helper()
	dir := t.TempDir()
	s, e := Open(dir, 2<<30, 7)
	if e != nil {
		t.Fatal(e)
	}
	now := time.Now().UTC().Add(-time.Minute)
	version := "0.0.9"
	session := capture.Session{ID: "session", PeerID: strings.Repeat("a", 128), IP: "8.8.8.8", Port: 35995, Version: &version, Capabilities: []string{"eth/61"}, Started: now.Add(-time.Minute), Ready: now.Add(-time.Minute), StartMono: 1}
	a := capture.Arrival{Type: "arrival", ObserverID: "observer", BootID: "boot", SessionID: session.ID, PeerID: session.PeerID, Hash: types.NewHash([]byte("momentum")), Kind: "hash_announcement", UTC: now, Mono: 123456789, Exclusion: "unresolved"}
	e = s.WriteBatch(records(map[string]any{"type": "run_started", "observerId": "observer", "bootId": "boot", "startedAt": now.Add(-time.Hour)}, map[string]any{"type": "session_started", "bootId": "boot", "session": session}, a))
	if e != nil {
		t.Fatal(e)
	}
	return s, dir, a
}
func TestResolutionRestartAndOriginalArrival(t *testing.T) {
	s, dir, a := fixture(t)
	h := protocol.Header{Hash: a.Hash, Height: 42, PreviousHash: types.NewHash([]byte("previous")), Timestamp: uint64(a.UTC.Unix()), Producer: "producer", Version: 1, Verification: "signature_checked"}
	resolved := a
	resolved.Type = "arrival_resolved"
	resolved.Header = &h
	resolved.Eligible = true
	resolved.Exclusion = ""
	if e := s.WriteBatch(records(resolved, map[string]any{"type": "duplicate_summary", "sessionId": a.SessionID, "hash": a.Hash, "eventKind": a.Kind, "duplicateCount": 3})); e != nil {
		t.Fatal(e)
	}
	// A repeated observation outside the in-memory window cannot replace clocks.
	late := resolved
	late.Type = "arrival"
	late.UTC = late.UTC.Add(time.Hour)
	late.Mono += int64(time.Hour)
	if e := s.WriteBatch(records(late)); e != nil {
		t.Fatal(e)
	}
	s.Close()
	s, e := Open(dir, 2<<30, 7)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	p, e := s.Collection(context.Background(), "arrivals", Query{Start: a.UTC.Add(-time.Second), End: a.UTC.Add(time.Minute), Limit: 100, Hash: a.Hash.String()})
	if e != nil {
		t.Fatal(e)
	}
	if len(p.Data) != 1 {
		t.Fatalf("duplicate records: %s", p.Data)
	}
	var got map[string]any
	json.Unmarshal(p.Data[0], &got)
	if got["arrivedAt"] != a.UTC.Format(time.RFC3339Nano) || int64(got["monotonicNs"].(float64)) != a.Mono || got["eligible"] != true || got["duplicateCount"] != float64(3) {
		t.Fatal(got)
	}
	peers, e := s.Collection(context.Background(), "peers", Query{Start: a.UTC.Add(-time.Hour), End: a.UTC.Add(time.Hour), Limit: 100})
	if e != nil {
		t.Fatal(e)
	}
	if !strings.Contains(string(peers.Data[0]), "unclean_shutdown") {
		t.Fatal("old session still appears connected")
	}
	var count int
	s.read.QueryRow("SELECT count(*) FROM arrivals").Scan(&count)
	if count != 1 {
		t.Fatal(count)
	}
}
func TestDisagreementRetainsCompetingHashesAndRepairs(t *testing.T) {
	s, _, a := fixture(t)
	defer s.Close()
	now := time.Now()
	h := protocol.Header{Hash: a.Hash, Height: 42, Timestamp: uint64(now.Unix()), Verification: "rpc_observed"}
	other := h
	other.Hash = types.NewHash([]byte("fork"))
	if e := s.WriteBatch(records(map[string]any{"type": "header", "header": h, "at": now}, map[string]any{"type": "header", "header": other, "at": now}, map[string]any{"type": "chain_agreement", "height": 42, "disputed": true, "at": now})); e != nil {
		t.Fatal(e)
	}
	var n int
	s.read.QueryRow("SELECT count(*) FROM momentums WHERE verification='disputed'").Scan(&n)
	if n != 2 {
		t.Fatal(n)
	}
	if e := s.WriteBatch(records(map[string]any{"type": "chain_agreement", "height": 42, "agreedHash": h.Hash.String(), "at": now})); e != nil {
		t.Fatal(e)
	}
	s.read.QueryRow("SELECT count(*) FROM momentums WHERE verification='rpc_agreed'").Scan(&n)
	if n != 1 {
		t.Fatal(n)
	}
	s.read.QueryRow("SELECT count(*) FROM momentums").Scan(&n)
	if n != 2 {
		t.Fatal("discarded competing hash")
	}
}

func TestSignerConflictCannotBecomeAnAgreedProducer(t *testing.T) {
	s, _, a := fixture(t)
	defer s.Close()
	now := time.Now()
	h := protocol.Header{Hash: a.Hash, Height: 44, Producer: "first", Verification: "signature_checked"}
	other := h
	other.Producer = "second"
	e := s.WriteBatch(records(map[string]any{"type": "header", "header": h, "at": now}, map[string]any{"type": "header", "header": other, "at": now}, map[string]any{"type": "chain_agreement", "height": 44, "agreedHash": h.Hash.String(), "at": now}))
	if e != nil {
		t.Fatal(e)
	}
	var state string
	var conflict int
	if e = s.read.QueryRow("SELECT verification,signer_conflict FROM momentums WHERE hash=?", h.Hash.String()).Scan(&state, &conflict); e != nil {
		t.Fatal(e)
	}
	if state != "disputed" || conflict != 1 {
		t.Fatal(state, conflict)
	}
}

func TestHalfOpenMomentumWindows(t *testing.T) {
	s, _, a := fixture(t)
	defer s.Close()
	base := a.UTC.Truncate(time.Second)
	h := protocol.Header{Hash: a.Hash, Height: 43, Timestamp: uint64(base.Unix()), Verification: "signature_checked"}
	if e := s.WriteBatch(records(map[string]any{"type": "header", "header": h, "at": a.UTC})); e != nil {
		t.Fatal(e)
	}
	p, e := s.Collection(context.Background(), "momentums", Query{Start: base.Add(500 * time.Millisecond), End: base.Add(time.Second), Limit: 10})
	if e != nil || len(p.Data) != 0 {
		t.Fatal("included before lower bound", p, e)
	}
	p, e = s.Collection(context.Background(), "momentums", Query{Start: base.Add(-time.Second), End: base.Add(500 * time.Millisecond), Limit: 10})
	if e != nil || len(p.Data) != 1 {
		t.Fatal("excluded before upper bound", p, e)
	}
}

func TestHistoricalEndpointAndVersionAreNotRewritten(t *testing.T) {
	s, _, a := fixture(t)
	defer s.Close()
	newVersion := "0.1.0"
	now := time.Now().UTC()
	changed := capture.Session{ID: "replacement", PeerID: a.PeerID, IP: "1.1.1.1", Port: 35995, Version: &newVersion, Started: now, Ready: now}
	if e := s.WriteBatch(records(map[string]any{"type": "session_started", "bootId": "boot", "session": changed}, map[string]any{"type": "chain_continuity_lost"})); e != nil {
		t.Fatal(e)
	}
	p, e := s.Collection(context.Background(), "peers", Query{Start: now.Add(-time.Hour), End: now.Add(time.Second), Limit: 100, Peer: a.PeerID})
	if e != nil || len(p.Data) != 2 {
		t.Fatal(p, e)
	}
	if !strings.Contains(string(p.Data[0]), "0.1.0") || !strings.Contains(string(p.Data[1]), "0.0.9") {
		t.Fatal(p)
	}
	if !s.ContinuityUnresolved(context.Background()) {
		t.Fatal("continuity loss not persisted")
	}
}
func TestMigrationRetentionRollbackAndBackup(t *testing.T) {
	s, dir, a := fixture(t)
	defer s.Close()
	if e := os.WriteFile(filepath.Join(dir, "identity.key"), []byte("keep"), 0600); e != nil {
		t.Fatal(e)
	}
	batch := [][]byte{}
	for i := 0; i < 1100; i++ {
		old := a
		old.Hash = types.NewHash([]byte{byte(i), byte(i >> 8)})
		old.UTC = time.Now().Add(-8 * 24 * time.Hour)
		batch = append(batch, records(old)...)
	}
	if e := s.WriteBatch(batch); e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 2; i++ {
		if e := s.Maintenance(context.Background(), time.Now()); e != nil {
			t.Fatal(e)
		}
	}
	var n int
	s.read.QueryRow("SELECT count(*) FROM arrivals").Scan(&n)
	if n != 1 {
		t.Fatal("retention count", n)
	}
	old := a
	old.Hash = types.NewHash([]byte("rollback"))
	if e := s.WriteBatch(records(old, map[string]any{"type": "invalid"})); e == nil {
		t.Fatal("invalid transaction accepted")
	}
	s.read.QueryRow("SELECT count(*) FROM arrivals").Scan(&n)
	if n != 1 {
		t.Fatal("partial transaction persisted")
	}
	ctx, end := context.WithTimeout(context.Background(), 5*time.Second)
	defer end()
	if e := Backup(ctx, dir, "snapshot.sqlite", 2<<30); e != nil {
		t.Fatal(e)
	}
	if _, e := os.Stat(filepath.Join(dir, "snapshot.sqlite")); e != nil {
		t.Fatal(e)
	}
	if e := Backup(ctx, dir, "snapshot.sqlite", 2<<30); e == nil {
		t.Fatal("overwrote backup")
	}
	if b, e := os.ReadFile(filepath.Join(dir, "identity.key")); e != nil || string(b) != "keep" {
		t.Fatal("identity altered")
	}
	if _, e := s.write.Exec("PRAGMA user_version=999"); e != nil {
		t.Fatal(e)
	}
	if _, e := Open(dir, 2<<30, 7); e == nil {
		t.Fatal("accepted future schema")
	}
}
