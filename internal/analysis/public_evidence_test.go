package analysis

import (
	"encoding/json"
	"os"
	"testing"
	"time"
)

// Public API evidence is a deliberately bounded compatibility sample, not an
// unbiased sample or a basis for advancing any association presentation label.
func TestPublicEvidenceReplay(t *testing.T) {
	b, e := os.ReadFile("../../docs/evidence/2026-11-11-associations/public-sample.json")
	if e != nil {
		t.Fatal(e)
	}
	var source struct {
		Start  time.Time `json:"windowStart"`
		End    time.Time `json:"windowEnd"`
		Status struct {
			Generated time.Time `json:"generatedAt"`
			Result    struct {
				Boot   string `json:"bootId"`
				Drops  int    `json:"captureDrops"`
				Clocks int    `json:"clockJumps"`
			}
		} `json:"status"`
		Peers []struct {
			ID        string     `json:"sessionId"`
			Boot      string     `json:"bootId"`
			Peer      string     `json:"peerId"`
			IP        string     `json:"ip"`
			Version   *string    `json:"advertisedVersion"`
			Ready     time.Time  `json:"versionObservedAt"`
			StartMono int64      `json:"statusCompletedMonoNs"`
			EndMono   *int64     `json:"disconnectedMonoNs"`
			Ended     *time.Time `json:"disconnectedAt"`
		} `json:"peers"`
		Arrivals []struct {
			Boot         string    `json:"bootId"`
			Session      string    `json:"sessionId"`
			Hash         string    `json:"hash"`
			Kind         string    `json:"eventKind"`
			At           time.Time `json:"arrivedAt"`
			Mono         int64     `json:"monotonicNs"`
			Eligible     bool      `json:"eligible"`
			Producer     string    `json:"producerAddress"`
			Verification string    `json:"verificationState"`
		} `json:"arrivals"`
	}
	if e = json.Unmarshal(b, &source); e != nil {
		t.Fatal(e)
	}
	runs := []Run{{Boot: source.Status.Result.Boot, LastSeen: source.Status.Generated, Unsafe: source.Status.Result.Drops != 0 || source.Status.Result.Clocks != 0}}
	sessions := []Session{}
	for _, v := range source.Peers {
		s := Session{ID: v.ID, Boot: v.Boot, Peer: v.Peer, IP: v.IP, Version: v.Version, Ready: v.Ready, StartMono: v.StartMono, Ended: v.Ended != nil}
		if v.EndMono != nil {
			s.EndMono = *v.EndMono
		}
		sessions = append(sessions, s)
	}
	arrivals := []Arrival{}
	for _, a := range source.Arrivals {
		arrivals = append(arrivals, Arrival{Boot: a.Boot, Session: a.Session, Hash: a.Hash, Kind: a.Kind, At: a.At, Mono: a.Mono, Eligible: a.Eligible, Producer: a.Producer, Verification: a.Verification})
	}
	if len(arrivals) != 190 || len(sessions) != 4 {
		t.Fatal("public evidence sample truncated", len(arrivals), len(sessions))
	}
	parts, excluded := Compute(source.Start, source.End, runs, sessions, arrivals)
	if len(parts) == 0 || len(excluded) != 0 {
		t.Fatal(len(parts), excluded)
	}
	report := NewReport(source.Start, source.End)
	report.AddHour(parts)
	rows := report.Finish()
	eligible, received := 0, 0
	for _, row := range rows {
		if row.ProducerVersion != nil || row.Status != "insufficient_data" || row.Holdout.Result != "insufficient_data" || row.Kind == "requested_response" {
			t.Fatal(row)
		}
		eligible += row.Eligible
		received += row.Received
	}
	if len(rows) != 168 || eligible != 168 || received != 125 {
		t.Fatal("public evidence totals changed", len(rows), eligible, received)
	}
	t.Logf("public compatibility sample: %d arrivals, %d association rows, %d eligible peer comparisons, %d deliveries; all insufficient_data", len(arrivals), len(rows), eligible, received)
}
