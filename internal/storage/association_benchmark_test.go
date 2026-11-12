package storage

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/0x3639/go-zenon-pillar-version/internal/analysis"
	"testing"
	"time"
)

// Models a seven-day, four-peer history with two independent event categories.
// Run without the race detector to measure the deployed query workload.
func BenchmarkAssociationWeek(b *testing.B) {
	s, e := Open(b.TempDir(), 2<<30, 7)
	if e != nil {
		b.Fatal(e)
	}
	defer s.Close()
	end := time.Now().UTC().Add(-time.Hour).Truncate(time.Hour)
	start := end.Add(-7 * 24 * time.Hour)
	tx, e := s.write.Begin()
	if e != nil {
		b.Fatal(e)
	}
	stmt, e := tx.Prepare("INSERT INTO association_parts(hour,cohort,producer,peer,kind,boot_id,payload) VALUES(?,?,?,?,?,?,?)")
	if e != nil {
		b.Fatal(e)
	}
	defer stmt.Close()
	for h := 0; h < 168; h++ {
		hour := start.Add(time.Duration(h) * time.Hour)
		if _, e = tx.Exec("INSERT INTO association_hours(hour,computed_revision,computed_at) VALUES(?,1,?)", hour.Unix(), end.UnixMilli()); e != nil {
			b.Fatal(e)
		}
		for producer := 0; producer < 86; producer++ {
			for peer := 0; peer < 4; peer++ {
				for _, kind := range []string{"hash_announcement", "full_broadcast"} {
					p := analysis.Part{Hour: hour, Boot: "boot", Cohort: "cohort", Members: []string{"a", "b", "c", "d"}, Producer: fmt.Sprintf("producer-%03d", producer), Peer: fmt.Sprint(peer), IP: fmt.Sprint(peer), Kind: kind, Counts: analysis.Counts{Eligible: 4, Received: 4, First: 1}, Other: &analysis.Counts{Eligible: 340, Received: 340, First: 85}, Delays: []int64{1000000, 2000000, 3000000, 4000000}, FirstAt: hour, LastAt: hour.Add(time.Minute), ConnectedSeconds: 3600}
					raw, _ := json.Marshal(p)
					if _, e = stmt.Exec(hour.Unix(), p.Cohort, p.Producer, p.Peer, p.Kind, p.Boot, string(raw)); e != nil {
						b.Fatal(e)
					}
				}
			}
		}
	}
	if e = tx.Commit(); e != nil {
		b.Fatal(e)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		page, e := s.Associations(context.Background(), Query{Start: start, End: end, Limit: 100})
		if e != nil {
			b.Fatal(e)
		}
		if len(page.Data) != 100 {
			b.Fatal(len(page.Data))
		}
	}
}
