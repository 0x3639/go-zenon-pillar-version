// Package analysis compares relay delivery at one observer. It never infers ownership.
package analysis

import (
	"context"
	"crypto/sha256"
	"fmt"
	"sort"
	"strings"
	"time"
)

const Method = "association-1"
const Window = 10 * time.Second
const Tie = 5 * time.Millisecond
const Warmup = 5 * time.Second

type Run struct {
	Boot     string
	LastSeen time.Time
	Unsafe   bool
}
type Session struct {
	ID, Boot, Peer, IP string
	Version            *string
	Ready              time.Time
	StartMono, EndMono int64
	Ended              bool
}
type Arrival struct {
	Boot, Session, Hash, Kind, Producer, Verification string
	At                                                time.Time
	Mono                                              int64
	Eligible, SignerConflict                          bool
}
type Counts struct {
	Eligible int `json:"eligibleCount"`
	Received int `json:"receivedCount"`
	First    int `json:"firstArrivalCount"`
	Ties     int `json:"tieCount"`
}

func (c *Counts) Add(b Counts) {
	c.Eligible += b.Eligible
	c.Received += b.Received
	c.First += b.First
	c.Ties += b.Ties
}
func (c Counts) Sub(b Counts) Counts {
	return Counts{c.Eligible - b.Eligible, c.Received - b.Received, c.First - b.First, c.Ties - b.Ties}
}

// Part is an hourly, cohort-specific sufficient statistic. Delays remain exact
// monotonic differences; hourly partitions support matched baselines and holdouts.
type Part struct {
	Hour      time.Time `json:"hour"`
	Boot      string    `json:"bootId"`
	Cohort    string    `json:"cohortId"`
	Members   []string  `json:"cohortSessionIds"`
	Producer  string    `json:"producerAddress"`
	Peer      string    `json:"peerId"`
	IP        string    `json:"ip"`
	Version   *string   `json:"advertisedVersion"`
	VersionAt time.Time `json:"versionObservedAt"`
	Kind      string    `json:"eventKind"`
	Counts
	Other            *Counts    `json:"otherProducers"`
	Delays           []int64    `json:"delaysNs"`
	ConnectedSeconds float64    `json:"connectedDurationSeconds"`
	FirstAt          time.Time  `json:"firstObservedAt"`
	LastAt           time.Time  `json:"lastObservedAt"`
	Name             *string    `json:"pillarName"`
	NameAt           *time.Time `json:"pillarNameObservedAt"`
}

func (p Part) Key() string { return p.Cohort + ":" + p.Kind + ":" + p.Producer + ":" + p.Peer }

// Compute includes a missing delivery in the denominator when that peer was
// continuously eligible. Restarts and session/IP/version changes split cohorts.
func Compute(start, end time.Time, runs []Run, sessions []Session, arrivals []Arrival) ([]Part, map[string]int) {
	parts, excluded, _ := ComputeContext(context.Background(), start, end, runs, sessions, arrivals)
	return parts, excluded
}

func ComputeContext(ctx context.Context, start, end time.Time, runs []Run, sessions []Session, arrivals []Arrival) ([]Part, map[string]int, error) {
	excluded := map[string]int{}
	rm := map[string]Run{}
	for _, r := range runs {
		rm[r.Boot] = r
	}
	sm := map[string]Session{}
	for _, s := range sessions {
		sm[s.ID] = s
	}
	groups := map[string][]Arrival{}
	for _, a := range arrivals {
		if a.Kind != "hash_announcement" && a.Kind != "full_broadcast" {
			continue
		}
		key := a.Boot + ":" + a.Hash + ":" + a.Kind
		groups[key] = append(groups[key], a)
	}
	parts := map[string]*Part{}
	durations := map[string]float64{}
	for _, aa := range groups {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		sort.Slice(aa, func(i, j int) bool { return aa[i].Mono < aa[j].Mono })
		a := aa[0]
		if a.At.Before(start) || !a.At.Before(end) {
			continue
		}
		r, ok := rm[a.Boot]
		if !ok || r.Unsafe {
			excluded["unsafe_or_unknown_run"]++
			continue
		}
		if a.Verification != "rpc_agreed" || a.SignerConflict || a.Producer == "" {
			excluded["unresolved_or_disputed"]++
			continue
		}
		members := []Session{}
		ids := []string{}
		peers := map[string]bool{}
		ips := map[string]bool{}
		ambiguous := false
		for _, s := range sessions {
			if s.Boot != a.Boot || s.StartMono+int64(Warmup) > a.Mono {
				continue
			}
			finish := s.EndMono
			if !s.Ended {
				finish = s.StartMono + r.LastSeen.Sub(s.Ready).Nanoseconds()
			}
			if finish < a.Mono+int64(Window) {
				continue
			}
			if peers[s.Peer] || ips[s.IP] {
				ambiguous = true
			}
			peers[s.Peer] = true
			ips[s.IP] = true
			members = append(members, s)
			ids = append(ids, s.ID)
		}
		if ambiguous || len(members) < 2 {
			excluded["fewer_than_two_distinct_eligible_peers"]++
			continue
		}
		sort.Strings(ids)
		cohort := fmt.Sprintf("%x", sha256.Sum256([]byte(a.Boot+"\n"+strings.Join(ids, "\n"))))[:24]
		duration, found := durations[cohort]
		if !found {
			var err error
			duration, err = cohortDuration(ctx, start, end, a.Boot, ids, sessions, r)
			if err != nil {
				return nil, nil, err
			}
			durations[cohort] = duration
		}
		eligible := map[string]bool{}
		for _, s := range members {
			eligible[s.ID] = true
		}
		delivery := map[string]Arrival{}
		earliest := int64(1<<63 - 1)
		for _, v := range aa {
			if !eligible[v.Session] || !v.Eligible || v.Mono < a.Mono || v.Mono > a.Mono+int64(Window) {
				continue
			}
			s := sm[v.Session]
			if old, ok := delivery[s.Peer]; ok && old.Mono <= v.Mono {
				continue
			}
			delivery[s.Peer] = v
			if v.Mono < earliest {
				earliest = v.Mono
			}
		}
		if len(delivery) == 0 {
			excluded["no_eligible_delivery"]++
			continue
		}
		ties := 0
		for _, v := range delivery {
			if v.Mono-earliest <= int64(Tie) {
				ties++
			}
		}
		for _, s := range members {
			p := Part{Hour: start, Boot: a.Boot, Cohort: cohort, Members: ids, Producer: a.Producer, Peer: s.Peer, IP: s.IP, Version: s.Version, VersionAt: s.Ready, Kind: a.Kind, ConnectedSeconds: duration, FirstAt: a.At, LastAt: a.At}
			key := p.Key()
			dst := parts[key]
			if dst == nil {
				dst = &p
				parts[key] = dst
			}
			dst.Eligible++
			if a.At.Before(dst.FirstAt) {
				dst.FirstAt = a.At
			}
			if a.At.After(dst.LastAt) {
				dst.LastAt = a.At
			}
			if v, ok := delivery[s.Peer]; ok {
				dst.Received++
				delay := v.Mono - earliest
				dst.Delays = append(dst.Delays, delay)
				if delay <= int64(Tie) {
					if ties == 1 {
						dst.First++
					} else {
						dst.Ties++
					}
				}
			}
		}
	}
	totals := map[string]Counts{}
	for _, p := range parts {
		k := p.Cohort + ":" + p.Kind + ":" + p.Peer
		c := totals[k]
		c.Add(p.Counts)
		totals[k] = c
	}
	out := make([]Part, 0, len(parts))
	for _, p := range parts {
		c := totals[p.Cohort+":"+p.Kind+":"+p.Peer].Sub(p.Counts)
		p.Other = &c
		out = append(out, *p)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key() < out[j].Key() })
	return out, excluded, nil
}

// cohortDuration measures the anchor-time intervals having exactly these eligible
// sessions, including the ten-second continuous-membership requirement.
func cohortDuration(ctx context.Context, start, end time.Time, boot string, ids []string, sessions []Session, run Run) (float64, error) {
	type interval struct {
		id         string
		start, end time.Time
	}
	spans := []interval{}
	bounds := []time.Time{start, end}
	for _, s := range sessions {
		if s.Boot != boot {
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
		spans = append(spans, interval{s.ID, l, r})
		bounds = append(bounds, l, r)
	}
	sort.Slice(bounds, func(i, j int) bool { return bounds[i].Before(bounds[j]) })
	seconds := 0.0
	for i := 1; i < len(bounds); i++ {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		l, r := bounds[i-1], bounds[i]
		if !r.After(l) {
			continue
		}
		mid := l.Add(r.Sub(l) / 2)
		members := []string{}
		for _, s := range spans {
			if !mid.Before(s.start) && mid.Before(s.end) {
				members = append(members, s.id)
			}
		}
		sort.Strings(members)
		if strings.Join(members, "\n") == strings.Join(ids, "\n") {
			seconds += r.Sub(l).Seconds()
		}
	}
	return seconds, nil
}
