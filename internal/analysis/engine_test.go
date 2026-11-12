package analysis

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func fixture() (time.Time, []Run, []Session, Arrival) {
	start := time.Date(2026, 11, 8, 0, 0, 0, 0, time.UTC)
	runs := []Run{{Boot: "boot", LastSeen: start.Add(2 * time.Hour)}}
	version := "0.0.9"
	sessions := []Session{{ID: "a", Boot: "boot", Peer: "a", IP: "1", Version: &version, Ready: start, StartMono: 0}, {ID: "b", Boot: "boot", Peer: "b", IP: "2", Ready: start, StartMono: 0}, {ID: "c", Boot: "boot", Peer: "c", IP: "3", Ready: start, StartMono: 0}}
	a := Arrival{Boot: "boot", Session: "a", Hash: "one", Kind: "hash_announcement", Producer: "pillar", Verification: "rpc_agreed", At: start.Add(time.Minute), Mono: int64(time.Minute), Eligible: true}
	return start, runs, sessions, a
}
func byPeer(t *testing.T, parts []Part, peer string) Part {
	t.Helper()
	for _, p := range parts {
		if p.Peer == peer {
			return p
		}
	}
	t.Fatalf("missing peer %s", peer)
	return Part{}
}
func TestTiesMissingDeliveryAndDeduplication(t *testing.T) {
	start, runs, sessions, a := fixture()
	b := a
	b.Session = "b"
	b.Mono += int64(5 * time.Millisecond)
	b.At = b.At.Add(5 * time.Millisecond)
	parts, _ := Compute(start, start.Add(time.Hour), runs, sessions, []Arrival{a, a, b})
	if len(parts) != 3 {
		t.Fatal(parts)
	}
	for _, p := range parts {
		if p.Eligible != 1 || p.First != 0 {
			t.Fatal(p)
		}
		if p.Peer == "c" {
			if p.Received != 0 || len(p.Delays) != 0 {
				t.Fatal(p)
			}
		} else if p.Ties != 1 || p.Received != 1 {
			t.Fatal(p)
		}
	}
	b.Mono++
	parts, _ = Compute(start, start.Add(time.Hour), runs, sessions, []Arrival{a, b})
	if byPeer(t, parts, "a").First != 1 || byPeer(t, parts, "b").Ties != 0 {
		t.Fatal(parts)
	}
}
func TestEligibilityBoundaries(t *testing.T) {
	start, runs, sessions, a := fixture()
	cases := []struct {
		name   string
		modify func(*[]Run, *[]Session, *[]Arrival)
		want   int
	}{
		{"requested", func(_ *[]Run, _ *[]Session, aa *[]Arrival) { (*aa)[0].Kind = "requested_response" }, 0},
		{"disputed", func(_ *[]Run, _ *[]Session, aa *[]Arrival) { (*aa)[0].Verification = "disputed" }, 0},
		{"signer conflict", func(_ *[]Run, _ *[]Session, aa *[]Arrival) { (*aa)[0].SignerConflict = true }, 0},
		{"capture gaps", func(r *[]Run, _ *[]Session, _ *[]Arrival) { (*r)[0].Unsafe = true }, 0},
		{"single peer", func(_ *[]Run, s *[]Session, _ *[]Arrival) { *s = (*s)[:1] }, 0},
		{"warmup", func(_ *[]Run, s *[]Session, _ *[]Arrival) {
			for i := range *s {
				(*s)[i].StartMono = int64(59 * time.Second)
			}
		}, 0},
		{"disconnected in window", func(_ *[]Run, s *[]Session, _ *[]Arrival) {
			(*s)[1].Ended = true
			(*s)[1].EndMono = int64(65 * time.Second)
		}, 2},
		{"no completed window", func(r *[]Run, _ *[]Session, _ *[]Arrival) { (*r)[0].LastSeen = start.Add(65 * time.Second) }, 0},
		{"other boot", func(_ *[]Run, s *[]Session, _ *[]Arrival) { (*s)[1].Boot = "other"; (*s)[2].Boot = "other" }, 0},
		{"duplicate IP", func(_ *[]Run, s *[]Session, _ *[]Arrival) { (*s)[1].IP = (*s)[0].IP }, 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rr := append([]Run{}, runs...)
			ss := append([]Session{}, sessions...)
			aa := []Arrival{a}
			c.modify(&rr, &ss, &aa)
			parts, _ := Compute(start, start.Add(time.Hour), rr, ss, aa)
			if len(parts) != c.want {
				t.Fatal(parts)
			}
		})
	}
}
func TestSeparateKindsVersionsCohortsAndBoots(t *testing.T) {
	start, runs, sessions, a := fixture()
	b := a
	b.Kind = "full_broadcast"
	parts, _ := Compute(start, start.Add(time.Hour), runs, sessions, []Arrival{a, b})
	if len(parts) != 6 {
		t.Fatal(parts)
	}
	first := byPeer(t, parts, "a")
	v := "0.1.0"
	sessions[0].ID = "a-new"
	sessions[0].Version = &v
	sessions[0].IP = "4"
	a.Session = "a-new"
	newer, _ := Compute(start, start.Add(time.Hour), runs, sessions, []Arrival{a})
	p := byPeer(t, newer, "a")
	if first.Cohort == p.Cohort || *first.Version == *p.Version || first.IP == p.IP {
		t.Fatal(first, p)
	}
}
func TestCrossHourWindowAndLateDelivery(t *testing.T) {
	start, runs, sessions, a := fixture()
	a.At = start.Add(time.Hour - time.Second)
	a.Mono = int64(time.Hour - time.Second)
	b := a
	b.Session = "b"
	b.Mono += int64(2 * time.Second)
	b.At = b.At.Add(2 * time.Second)
	c := a
	c.Session = "c"
	c.Mono += int64(11 * time.Second)
	c.At = c.At.Add(11 * time.Second)
	parts, _ := Compute(start, start.Add(time.Hour), runs, sessions, []Arrival{a, b, c})
	if byPeer(t, parts, "b").Received != 1 || byPeer(t, parts, "c").Received != 0 {
		t.Fatal(parts)
	}
	parts, _ = Compute(start.Add(time.Hour), start.Add(2*time.Hour), runs, sessions, []Arrival{a, b, c})
	if len(parts) != 0 {
		t.Fatal("repeated comparison", parts)
	}
}
func TestGeneralFastRelayVersusPillarSpecificHoldout(t *testing.T) {
	start, _, sessions, _ := fixture()
	report := NewReport(start, start.Add(4*24*time.Hour))
	for day := 0; day < 4; day++ {
		now := start.Add(time.Duration(day) * 24 * time.Hour)
		runs := []Run{{Boot: "boot", LastSeen: now.Add(2 * time.Hour)}}
		arrivals := []Arrival{}
		for n := 0; n < 120; n++ {
			producer := "other"
			if n%2 == 0 {
				producer = "target"
			}
			base := now.Add(time.Duration(n+1) * 20 * time.Second)
			for _, s := range sessions {
				delay := 20 * time.Millisecond
				if s.ID == "a" {
					delay = 10 * time.Millisecond
				}
				if s.ID == "b" && producer == "target" {
					delay = 0
				}
				arrivals = append(arrivals, Arrival{Boot: "boot", Session: s.ID, Hash: fmt.Sprint(day, "-", n), Kind: "hash_announcement", Producer: producer, Verification: "rpc_agreed", At: base.Add(delay), Mono: base.Add(delay).Sub(start).Nanoseconds(), Eligible: true})
			}
		}
		parts, _ := Compute(now, now.Add(time.Hour), runs, sessions, arrivals)
		report.AddHour(parts)
	}
	rows := report.Finish()
	for _, r := range rows {
		if r.SampleCount != 240 || r.Days != 4 || r.Status != "observed_relay_association" || r.ProducerVersion != nil {
			t.Fatal(r)
		}
		if r.Producer == "target" && r.Peer == "b" && r.Holdout.Result != "persistent_positive_pattern" {
			t.Fatal(r)
		}
		if r.Producer == "target" && r.Peer == "a" && r.Holdout.Result != "no_persistent_specific_advantage" {
			t.Fatal(r)
		}
	}
	// A relay winning for all producers has no excess over its own baseline.
	report = NewReport(start, start.Add(4*24*time.Hour))
	for d := 0; d < 4; d++ {
		hour := start.Add(time.Duration(d) * 24 * time.Hour)
		parts := []Part{}
		for _, producer := range []string{"p", "q"} {
			parts = append(parts, Part{Hour: hour, Cohort: "same", Peer: "fast", Producer: producer, Counts: Counts{Eligible: 100, Received: 100, First: 100}, FirstAt: hour, LastAt: hour})
		}
		report.AddHour(parts)
	}
	for _, r := range report.Finish() {
		if r.ExcessShare == nil || *r.ExcessShare != 0 || r.Holdout.Result != "no_persistent_specific_advantage" {
			t.Fatal(r)
		}
	}
}
func TestThresholdAndUnstablePattern(t *testing.T) {
	start, _, _, _ := fixture()
	report := NewReport(start, start.Add(4*24*time.Hour))
	for d := 0; d < 4; d++ {
		first := 30
		if d >= 2 {
			first = 0
		}
		hour := start.Add(time.Duration(d) * 24 * time.Hour)
		report.AddHour([]Part{{Hour: hour, Cohort: "c", Peer: "a", Producer: "target", Counts: Counts{Eligible: 30, Received: 30, First: first}, FirstAt: hour, LastAt: hour}, {Hour: hour, Cohort: "c", Peer: "a", Producer: "baseline", Counts: Counts{Eligible: 100, Received: 100}, FirstAt: hour, LastAt: hour}})
	}
	for _, r := range report.Finish() {
		if r.Producer == "target" && r.Holdout.Result != "unstable_pattern" {
			t.Fatal(r)
		}
	}
	report = NewReport(start, start.Add(4*24*time.Hour))
	report.AddHour([]Part{{Hour: start, Cohort: "c", Peer: "a", Producer: "target", Counts: Counts{Eligible: 1000}}})
	if report.Finish()[0].Status != "insufficient_data" {
		t.Fatal("one day accepted")
	}
}

func TestExactDelayStatisticsAndWeightedBaseline(t *testing.T) {
	start, _, _, _ := fixture()
	report := NewReport(start, start.Add(4*time.Hour))
	report.AddHour([]Part{{Hour: start, Cohort: "c", Producer: "p", Peer: "a", Counts: Counts{Eligible: 100, Received: 4, First: 2}, Other: &Counts{Eligible: 100, First: 100}, Delays: []int64{0, 2e6, 8e6, 10e6}}})
	report.AddHour([]Part{{Hour: start.Add(time.Hour), Cohort: "c", Producer: "p", Peer: "a", Counts: Counts{Eligible: 1}, Other: &Counts{Eligible: 100}}})
	row := report.Finish()[0]
	if row.Median == nil || *row.Median != 5 || *row.P95 != 10 || row.DeliveryCoverage != 4.0/101 || *row.BaselineShare != 100.0/101 || row.MatchedEligible != 101 {
		t.Fatal(row)
	}
	// Persisted other-producer counts still apply when no other rows are selected.
	if row.Baseline.Eligible != 200 || row.Baseline.First != 100 {
		t.Fatal(row)
	}
}

func TestAnalysisCancellation(t *testing.T) {
	start, runs, sessions, a := fixture()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, e := ComputeContext(ctx, start, start.Add(time.Hour), runs, sessions, []Arrival{a}); e == nil {
		t.Fatal("canceled analysis succeeded")
	}
}
