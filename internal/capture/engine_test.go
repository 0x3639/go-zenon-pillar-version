package capture

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/zenon-network/go-zenon/common/types"
)

func engine(t *testing.T) *Engine {
	t.Helper()
	w, e := NewWriter(filepath.Join(t.TempDir(), "evidence.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(w.Close)
	return NewEngine("test", "boot", w)
}
func TestPerPeerDedupAndDeferredResolution(t *testing.T) {
	e := engine(t)
	ready := time.Now().Add(-10 * time.Second)
	e.AddSession(Session{ID: "a", PeerID: "peer-a", IP: "8.8.8.8", Ready: ready})
	e.AddSession(Session{ID: "b", PeerID: "peer-b", IP: "1.1.1.1", Ready: ready})
	at := time.Now()
	hash := types.NewHash([]byte("m"))
	e.Observe("a", hash, "hash_announcement", at, nil)
	e.Observe("a", hash, "hash_announcement", at.Add(time.Second), nil)
	e.Observe("b", hash, "hash_announcement", at.Add(time.Millisecond), nil)
	if len(e.arrivals) != 2 {
		t.Fatal("global dedup lost a peer")
	}
	h := protocol.Header{Hash: hash, Timestamp: uint64(at.Unix()), Producer: "producer"}
	e.Resolved(h)
	a := e.arrivals["a:"+hash.String()+":hash_announcement"]
	if !a.UTC.Equal(at) || a.Mono != at.Sub(e.Start).Nanoseconds() || a.Duplicates != 1 || a.Header.Producer != "producer" {
		t.Fatal("resolution rewrote evidence")
	}
	r := e.Report(time.Now())
	if r["distinctCheckedLiveMomentumsSeenFromTwoPeers"].(int) != 1 {
		t.Fatal("lost peer observations")
	}
	if r["milestone1Passed"].(bool) {
		t.Fatal("premature pass")
	}
}
func TestRequestedResponseIsNeverUnsolicited(t *testing.T) {
	e := engine(t)
	at := time.Now()
	e.AddSession(Session{ID: "a", PeerID: "a", Ready: at.Add(-time.Minute)})
	h := protocol.Header{Hash: types.NewHash([]byte("h")), Timestamp: uint64(at.Unix())}
	e.Observe("a", h.Hash, "requested_response", at, &h)
	r := e.Report(at)
	if r["distinctCheckedLiveMomentums"].(int) != 0 {
		t.Fatal("requested response ranked")
	}
}
func TestSinglePeerCannotPass(t *testing.T) {
	e := engine(t)
	now := time.Now()
	e.Start = now.Add(-2 * time.Hour)
	e.AddSession(Session{ID: "a", PeerID: "a", IP: "8.8.8.8", Ready: now.Add(-time.Hour)})
	for i := 0; i < 100; i++ {
		h := protocol.Header{Hash: types.NewHash([]byte{byte(i)}), Timestamp: uint64(now.Unix())}
		e.Observe("a", h.Hash, "full_broadcast", now, &h)
	}
	if e.Report(now)["milestone1Passed"].(bool) {
		t.Fatal("single peer passed")
	}
}

func TestAcceptanceRequiresBothDurationAndCleanCapture(t *testing.T) {
	e := engine(t)
	now := time.Now()
	e.Start = now.Add(-62 * time.Minute)
	for _, id := range []string{"a", "b"} {
		ip := "8.8.8.8"
		if id == "b" {
			ip = "1.1.1.1"
		}
		e.AddSession(Session{ID: id, PeerID: id, IP: ip, Ready: now.Add(-61 * time.Minute)})
	}
	for i := 0; i < 100; i++ {
		h := protocol.Header{Hash: types.NewHash([]byte{byte(i)}), Timestamp: uint64(now.Unix())}
		for _, id := range []string{"a", "b"} {
			e.Observe(id, h.Hash, "full_broadcast", now, &h)
		}
	}
	if !e.Report(now)["milestone1Passed"].(bool) {
		t.Fatal("complete clean fixture did not pass")
	}
	if e.Report(now.Add(-2 * time.Minute))["milestone1Passed"].(bool) {
		t.Fatal("sub-hour fixture passed")
	}
	e.Writer.Drops.Add(1)
	if e.Report(now)["milestone1Passed"].(bool) {
		t.Fatal("capture drop was hidden")
	}
}
func TestQueueSaturationAndRecordLimits(t *testing.T) {
	w := &Writer{queue: make(chan []byte, 1)}
	if !w.Submit(map[string]int{"x": 1}) {
		t.Fatal("first write")
	}
	if w.Submit(map[string]int{"x": 2}) || w.Drops.Load() != 1 {
		t.Fatal("overflow not counted")
	}
	if w.Submit(make([]byte, MaxRecordBytes)) {
		t.Fatal("oversized record accepted")
	}
}
