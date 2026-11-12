package analysis

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"
	"time"
)

func TestWarmupAnchorKeepsAllMissesAndRejectsHistoricalAnchors(t *testing.T) {
	start, runs, sessions, a := fixture()
	sessions[2].Ready = start.Add(58 * time.Second)
	sessions[2].StartMono = int64(58 * time.Second)
	a.Session = "c"
	a.Eligible = false
	a.Slot = a.At
	parts, excluded := Compute(start, start.Add(time.Hour), runs, sessions, []Arrival{a})
	if len(parts) != 2 || len(excluded) != 0 {
		t.Fatal(parts, excluded)
	}
	report := NewReport(start, start.Add(time.Hour))
	report.AddHour(parts)
	for _, r := range report.Finish() {
		if r.Eligible != 1 || r.Received != 0 || r.First != 0 || r.Ties != 0 || r.Median != nil || r.P95 != nil || r.FirstShare != 0 || r.DeliveryCoverage != 0 {
			t.Fatal(r)
		}
	}
	for _, slot := range []time.Time{a.At.Add(-3 * time.Minute), a.At.Add(11 * time.Second), {}} {
		a.Slot = slot
		parts, excluded = Compute(start, start.Add(time.Hour), runs, sessions, []Arrival{a})
		if len(parts) != 0 || excluded["no_live_delivery_or_anchor"] != 1 {
			t.Fatal(slot, parts, excluded)
		}
	}
}

// Independent reference calculation retained only in tests to check the sweep.
func slowDuration(start, end time.Time, ids []string, sessions []Session, run Run) float64 {
	type span struct {
		id   string
		l, r time.Time
	}
	spans := []span{}
	bounds := []time.Time{start, end}
	for _, s := range sessions {
		if s.Boot != run.Boot {
			continue
		}
		l := s.Ready.Add(Warmup)
		r := run.LastSeen
		if s.Ended {
			r = s.Ready.Add(time.Duration(s.EndMono - s.StartMono))
		}
		r = r.Add(-Window)
		if l.Before(start) {
			l = start
		}
		if r.After(end) {
			r = end
		}
		if !r.After(l) {
			continue
		}
		spans = append(spans, span{s.ID, l, r})
		bounds = append(bounds, l, r)
	}
	sort.Slice(bounds, func(i, j int) bool { return bounds[i].Before(bounds[j]) })
	total := 0.0
	for i := 1; i < len(bounds); i++ {
		l, r := bounds[i-1], bounds[i]
		if !r.After(l) {
			continue
		}
		mid := l.Add(r.Sub(l) / 2)
		active := []string{}
		for _, s := range spans {
			if !mid.Before(s.l) && mid.Before(s.r) {
				active = append(active, s.id)
			}
		}
		sort.Strings(active)
		if strings.Join(active, "\n") == strings.Join(ids, "\n") {
			total += r.Sub(l).Seconds()
		}
	}
	return total
}
func TestDurationSweepMatchesIndependentReference(t *testing.T) {
	start, runs, sessions, _ := fixture()
	r := runs[0]
	sessions[0].Ended = true
	sessions[0].EndMono = int64(40 * time.Minute)
	sessions[1].Ready = start.Add(10 * time.Minute)
	sessions[1].StartMono = int64(10 * time.Minute)
	sessions = append(sessions, Session{ID: "d", Boot: "boot", Peer: "d", IP: "4", Ready: start.Add(40*time.Minute - 5*time.Second), StartMono: int64(40*time.Minute - 5*time.Second), Ended: true, EndMono: int64(time.Hour)})
	got, e := cohortDurations(context.Background(), start, start.Add(time.Hour), r.Boot, sessions, r)
	if e != nil {
		t.Fatal(e)
	}
	for mask := 1; mask < (1 << len(sessions)); mask++ {
		ids := []string{}
		for i, s := range sessions {
			if mask&(1<<i) != 0 {
				ids = append(ids, s.ID)
			}
		}
		if len(ids) < 2 {
			continue
		}
		sort.Strings(ids)
		want := slowDuration(start, start.Add(time.Hour), ids, sessions, r)
		if math.Abs(got[cohortID(r.Boot, ids)]-want) > 1e-9 {
			t.Fatal(ids, got, want)
		}
	}
}
func BenchmarkDenseCohortSweep(b *testing.B) {
	start := time.Date(2026, 11, 11, 0, 0, 0, 0, time.UTC)
	run := Run{Boot: "boot", LastSeen: start.Add(2 * time.Hour)}
	ss := []Session{}
	for group := 0; group < 224; group++ {
		begin := time.Duration(group) * 16 * time.Second
		for peer := 0; peer < 8; peer++ {
			d := 16 * time.Second
			if peer == 0 {
				d = 15100 * time.Millisecond
			}
			ss = append(ss, Session{ID: fmt.Sprintf("%d-%d", group, peer), Boot: "boot", Ready: start.Add(begin), StartMono: int64(begin), Ended: true, EndMono: int64(begin + d)})
		}
		if group < 128 {
			for n := 0; n < 2; n++ {
				t := begin + 15200*time.Millisecond + time.Duration(n)*300*time.Millisecond
				ss = append(ss, Session{ID: fmt.Sprintf("short-%d-%d", group, n), Boot: "boot", Ready: start.Add(t), StartMono: int64(t), Ended: true, EndMono: int64(t + 200*time.Millisecond)})
			}
		}
	}
	if len(ss) != 2048 {
		b.Fatal(len(ss))
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, e := cohortDurations(context.Background(), start, start.Add(time.Hour), "boot", ss, run)
		if e != nil {
			b.Fatal(e)
		}
	}
}
