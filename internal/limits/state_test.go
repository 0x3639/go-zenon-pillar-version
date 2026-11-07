package limits

import (
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestOversizedStateFailsClosed(t *testing.T) {
	path := filepath.Join(t.TempDir(), "limits.json")
	if e := os.WriteFile(path, make([]byte, 128*1024+1), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := Open(path, 1<<20, 1<<20); e == nil {
		t.Fatal("accepted oversized state")
	}
}

func TestReservationsAndBreakerSurviveRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	b, e := Open(path, 65536, 65536)
	if e != nil {
		t.Fatal(e)
	}
	if e = b.reserve(1, true); e != nil {
		t.Fatal(e)
	}
	restarted, e := Open(path, 65536, 65536)
	if e != nil {
		t.Fatal(e)
	}
	if e = restarted.reserve(1, true); e != ErrTraffic {
		t.Fatalf("restart reset budget: %v", e)
	}
	again, e := Open(path, 65536, 65536)
	if e != nil {
		t.Fatal(e)
	}
	if !again.Paused() {
		t.Fatal("lost breaker")
	}
}
func TestCooldownSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "state.json")
	b, e := Open(path, 1<<20, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	cd := Cooldown{Failures: 3, Until: time.Now().Add(time.Minute)}
	if e = b.SetCooldown("peer", cd); e != nil {
		t.Fatal(e)
	}
	b, e = Open(path, 1<<20, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	if b.Cooldown("peer").Failures != 3 || !b.Cooldown("peer").Until.Equal(cd.Until) {
		t.Fatal("lost cooldown")
	}
}
func TestRollingBudgetExpiresConservatively(t *testing.T) {
	b := &Budget{state: State{Buckets: []Bucket{{Minute: 100, In: 123}, {Minute: 101, In: 456}}}}
	b.prune(time.Unix(161*60, 0))
	in, _ := b.totals()
	if in != 456 {
		t.Fatalf("wrong expiry %d", in)
	}
}

func TestOldReadCannotRefundIntoNewMinute(t *testing.T) {
	b, e := Open(filepath.Join(t.TempDir(), "limits.json"), 1<<20, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	minute, e := b.reserveIO(32768, true)
	if e != nil {
		t.Fatal(e)
	}
	b.prune(time.Unix((minute+1)*60, 0))
	b.refund(32767, true, 1, minute)
	if b.creditIn != 0 {
		t.Fatal("old I/O restored expired credit")
	}
	if b.actualIn != 1 {
		t.Fatal("actual byte count lost")
	}
}

func TestStalledPersistenceFailsClosedWithoutUnboundedWorkers(t *testing.T) {
	b, e := Open(filepath.Join(t.TempDir(), "limits.json"), 1<<20, 1<<20)
	if e != nil {
		t.Fatal(e)
	}
	release := make(chan struct{})
	done := make(chan struct{})
	var calls atomic.Int32
	b.writeState = func(_ string, _ []byte) error { calls.Add(1); <-release; close(done); return nil }
	start := time.Now()
	if e = b.reserve(1, true); e == nil {
		t.Fatal("granted traffic before durable reservation")
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("storage stalled socket budget indefinitely")
	}
	if e = b.reserve(1, true); e == nil {
		t.Fatal("resumed after failed persistence")
	}
	if calls.Load() != 1 {
		t.Fatal("created additional blocked storage workers")
	}
	close(release)
	<-done
}
