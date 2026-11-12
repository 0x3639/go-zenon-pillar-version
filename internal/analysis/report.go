package analysis

import (
	"math"
	"sort"
	"time"
)

type Measurement struct {
	Counts
	FirstShare       float64  `json:"firstArrivalShare"`
	DeliveryCoverage float64  `json:"deliveryCoverage"`
	TieShare         float64  `json:"tieShare"`
	Median           *float64 `json:"medianRelativeDelayMs"`
	P95              *float64 `json:"p95RelativeDelayMs"`
	Baseline         Counts   `json:"otherProducers"`
	MatchedEligible  int      `json:"baselineMatchedEligibleCount"`
	BaselineShare    *float64 `json:"baselineFirstArrivalShare"`
	ExcessShare      *float64 `json:"excessFirstArrivalShare"`
	delays           []int64
	expected         float64
	matchedFirst     int
}

func (m *Measurement) add(p Part, other Counts) {
	m.Counts.Add(p.Counts)
	m.delays = append(m.delays, p.Delays...)
	m.Baseline.Add(other)
	if other.Eligible > 0 {
		m.MatchedEligible += p.Eligible
		m.matchedFirst += p.First
		m.expected += float64(p.Eligible) * float64(other.First) / float64(other.Eligible)
	}
}
func (m *Measurement) finish() {
	if m.Eligible > 0 {
		m.FirstShare = float64(m.First) / float64(m.Eligible)
		m.DeliveryCoverage = float64(m.Received) / float64(m.Eligible)
		m.TieShare = float64(m.Ties) / float64(m.Eligible)
	}
	if m.MatchedEligible > 0 {
		b := m.expected / float64(m.MatchedEligible)
		x := float64(m.matchedFirst)/float64(m.MatchedEligible) - b
		m.BaselineShare = &b
		m.ExcessShare = &x
	}
	if len(m.delays) > 0 {
		sort.Slice(m.delays, func(i, j int) bool { return m.delays[i] < m.delays[j] })
		n := len(m.delays)
		med := float64(m.delays[n/2]) / 1e6
		if n%2 == 0 {
			med = (float64(m.delays[n/2-1]) + float64(m.delays[n/2])) / 2e6
		}
		p95 := float64(m.delays[int(math.Ceil(.95*float64(n)))-1]) / 1e6
		m.Median = &med
		m.P95 = &p95
	}
	m.delays = nil
}

type Day struct {
	Day string `json:"utcDay"`
	Measurement
}
type Holdout struct {
	Split  time.Time   `json:"splitAt"`
	Early  Measurement `json:"earlier"`
	Later  Measurement `json:"later"`
	Result string      `json:"result"`
}
type Row struct {
	PillarName       *string    `json:"pillarName"`
	NameAt           *time.Time `json:"pillarNameObservedAt"`
	Producer         string     `json:"producerAddress"`
	Peer             string     `json:"peerId"`
	IP               string     `json:"ip"`
	Version          *string    `json:"advertisedVersion"`
	VersionAt        time.Time  `json:"versionObservedAt"`
	ProducerVersion  *string    `json:"producerSoftwareVersion"`
	Cohort           string     `json:"cohortId"`
	Boot             string     `json:"bootId"`
	Members          []string   `json:"cohortSessionIds"`
	Competitors      int        `json:"activeCompetitorCount"`
	Kind             string     `json:"eventKind"`
	SampleCount      int        `json:"sampleCount"`
	Days             int        `json:"utcDayCount"`
	ConnectedSeconds float64    `json:"connectedDurationSeconds"`
	FirstAt          time.Time  `json:"firstObservedAt"`
	LastAt           time.Time  `json:"lastObservedAt"`
	Status           string     `json:"associationStatus"`
	RawAvailable     bool       `json:"rawEvidenceAvailable"`
	Measurement
	PerDay  []Day   `json:"perDay"`
	Holdout Holdout `json:"holdout"`
	days    map[string]*Measurement
}
type Report struct {
	split time.Time
	rows  map[string]*Row
}

func NewReport(start, end time.Time) *Report {
	return &Report{split: start.Add(end.Sub(start) / 2).Truncate(time.Hour), rows: map[string]*Row{}}
}

// AddHour must receive all producers before filtering the output. Otherwise a
// producer filter would erase the other-producer baseline.
func (r *Report) AddHour(parts []Part) {
	totals := map[string]Counts{}
	for _, p := range parts {
		k := p.Cohort + ":" + p.Kind + ":" + p.Peer
		c := totals[k]
		c.Add(p.Counts)
		totals[k] = c
	}
	for _, p := range parts {
		key := p.Key()
		row := r.rows[key]
		if row == nil {
			row = &Row{PillarName: p.Name, NameAt: p.NameAt, Producer: p.Producer, Peer: p.Peer, IP: p.IP, Version: p.Version, VersionAt: p.VersionAt, Cohort: p.Cohort, Boot: p.Boot, Members: p.Members, Competitors: len(p.Members) - 1, Kind: p.Kind, FirstAt: p.FirstAt, LastAt: p.LastAt, days: map[string]*Measurement{}, Holdout: Holdout{Split: r.split}}
			r.rows[key] = row
		}
		if p.NameAt != nil && (row.NameAt == nil || p.NameAt.After(*row.NameAt)) {
			row.PillarName = p.Name
			row.NameAt = p.NameAt
		}
		other := totals[p.Cohort+":"+p.Kind+":"+p.Peer].Sub(p.Counts)
		if p.Other != nil {
			other = *p.Other
		}
		row.Measurement.add(p, other)
		row.ConnectedSeconds += p.ConnectedSeconds
		if p.FirstAt.Before(row.FirstAt) {
			row.FirstAt = p.FirstAt
		}
		if p.LastAt.After(row.LastAt) {
			row.LastAt = p.LastAt
		}
		day := p.Hour.UTC().Format("2006-01-02")
		if row.days[day] == nil {
			row.days[day] = &Measurement{}
		}
		row.days[day].add(p, other)
		if p.Hour.Before(r.split) {
			row.Holdout.Early.add(p, other)
		} else {
			row.Holdout.Later.add(p, other)
		}
	}
}
func (r *Report) Finish() []Row {
	out := []Row{}
	for _, row := range r.rows {
		row.SampleCount = row.Eligible
		row.Days = len(row.days)
		row.Status = "insufficient_data"
		if row.SampleCount >= 100 && row.Days >= 3 {
			row.Status = "observed_relay_association"
		}
		row.Measurement.finish()
		row.Holdout.Early.finish()
		row.Holdout.Later.finish()
		row.Holdout.Result = "insufficient_data"
		a, b := row.Holdout.Early, row.Holdout.Later
		enough := func(m Measurement) bool {
			return m.Eligible >= 20 && m.Baseline.Eligible >= 20 && m.MatchedEligible*5 >= m.Eligible*4 && m.ExcessShare != nil
		}
		if row.Status != "insufficient_data" && enough(a) && enough(b) {
			switch {
			case *a.ExcessShare > .05 && *b.ExcessShare > .05:
				row.Holdout.Result = "persistent_positive_pattern"
			case *a.ExcessShare <= .05 && *b.ExcessShare <= .05:
				row.Holdout.Result = "no_persistent_specific_advantage"
			default:
				row.Holdout.Result = "unstable_pattern"
			}
		}
		for d, m := range row.days {
			m.finish()
			row.PerDay = append(row.PerDay, Day{d, *m})
		}
		sort.Slice(row.PerDay, func(i, j int) bool { return row.PerDay[i].Day < row.PerDay[j].Day })
		row.days = nil
		out = append(out, *row)
	}
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		return a.Producer+a.Peer+a.Cohort+a.Kind < b.Producer+b.Peer+b.Cohort+b.Kind
	})
	return out
}
