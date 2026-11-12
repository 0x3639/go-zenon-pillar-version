package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/analysis"
	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/zenon-network/go-zenon/common/types"
)

func TestStoredWarmupAnchorCountsMissesOnlyForLiveSlot(t *testing.T) {
	s, start, _ := associationFixture(t)
	if _, e := s.write.Exec(`DELETE FROM arrivals WHERE session_id='b';
 UPDATE arrivals SET eligible=0;
 UPDATE sessions SET ready_at=?,start_mono=? WHERE session_id='a'`, start.Add(58*time.Second).UnixMilli(), int64(58*time.Second)); e != nil {
		t.Fatal(e)
	}
	analyzeAll(t, s)
	query := Query{Start: start, End: start.Add(time.Hour), Limit: 100}
	page, e := s.Associations(context.Background(), query)
	if e != nil || len(page.Data) != 2 {
		t.Fatal(page, e)
	}
	for _, row := range page.Data {
		if row.Eligible != 1 || row.Received != 0 || row.Median != nil || row.P95 != nil {
			t.Fatal(row)
		}
	}
	if _, e = s.write.Exec(`UPDATE momentums SET timestamp=?;
 UPDATE association_hours SET revision=revision+1`, start.Add(-3*time.Minute).Unix()); e != nil {
		t.Fatal(e)
	}
	analyzeAll(t, s)
	page, e = s.Associations(context.Background(), query)
	if e != nil || len(page.Data) != 0 || page.Exclusions["no_live_delivery_or_anchor"] != 1 {
		t.Fatal(page, e)
	}
}

func TestExactSessionRetention(t *testing.T) {
	s, start, _ := associationFixture(t)
	analyzeAll(t, s)
	old := time.Now().Add(-120 * 24 * time.Hour)
	if _, e := s.write.Exec(`INSERT INTO sessions(session_id,boot_id,peer_id,ip,port,client_name,capabilities,started_at,ready_at,start_mono,ended_at,end_mono) VALUES('orphan','boot','other','1.2.3.4',1,'','[]',?,?,0,?,?)`, old.UnixMilli(), old.UnixMilli(), old.Add(time.Hour).UnixMilli(), int64(time.Hour)); e != nil {
		t.Fatal(e)
	}
	if _, e := s.write.Exec("UPDATE sessions SET ended_at=?,end_mono=? WHERE session_id<>'orphan'", start.Add(2*time.Hour).UnixMilli(), int64(2*time.Hour)); e != nil {
		t.Fatal(e)
	}
	if e := s.Maintenance(context.Background(), time.Now().Add(8*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	var n int
	s.read.QueryRow("SELECT count(*) FROM sessions WHERE session_id='orphan'").Scan(&n)
	if n != 0 {
		t.Fatal("unreferenced session retained")
	}
	s.read.QueryRow("SELECT count(*) FROM sessions").Scan(&n)
	if n != 3 {
		t.Fatal("referenced cohort member removed", n)
	}
	if e := s.Maintenance(context.Background(), time.Now().Add(100*24*time.Hour)); e != nil {
		t.Fatal(e)
	}
	s.read.QueryRow("SELECT count(*) FROM sessions").Scan(&n)
	if n != 0 {
		t.Fatal("references outlived summaries", n)
	}
}
func TestTransientHourFailureDoesNotBlockLaterWork(t *testing.T) {
	s, start, _ := associationFixture(t)
	if _, e := s.write.Exec("INSERT INTO association_hours(hour) VALUES(?)", start.Add(time.Hour).Unix()); e != nil {
		t.Fatal(e)
	}
	if _, e := s.write.Exec("CREATE TRIGGER fail_part BEFORE INSERT ON association_parts BEGIN SELECT RAISE(FAIL,'simulated transient write failure'); END"); e != nil {
		t.Fatal(e)
	}
	now := time.Now()
	if _, e := s.AnalyzeNext(context.Background(), now); e == nil {
		t.Fatal("failure not injected")
	}
	page, e := s.Associations(context.Background(), Query{Start: start, End: start.Add(2 * time.Hour), Limit: 100})
	if e != nil || page.PendingHours != 2 || page.RetryingHours != 1 {
		t.Fatal(page, e)
	}
	if ok, e := s.AnalyzeNext(context.Background(), now); e != nil || !ok {
		t.Fatal(ok, e)
	}
	page, e = s.Associations(context.Background(), Query{Start: start, End: start.Add(2 * time.Hour), Limit: 100})
	if e != nil || page.ProcessedHours != 1 || page.PendingHours != 1 {
		t.Fatal(page, e)
	}
	if _, e = s.write.Exec("DROP TRIGGER fail_part; UPDATE association_hours SET revision=revision+1 WHERE hour=?", start.Unix()); e != nil {
		t.Fatal(e)
	}
	if ok, e := s.AnalyzeNext(context.Background(), now); e != nil || !ok {
		t.Fatal(ok, e)
	}
	page, e = s.Associations(context.Background(), Query{Start: start, End: start.Add(2 * time.Hour), Limit: 100})
	if e != nil || len(page.Data) != 3 || page.PendingHours != 0 {
		t.Fatal(page, e)
	}
}
func TestRunBoundIsTerminalForOnlyItsHour(t *testing.T) {
	s, start, _ := associationFixture(t)
	tx, e := s.write.Begin()
	if e != nil {
		t.Fatal(e)
	}
	for i := 0; i < 129; i++ {
		_, e = tx.Exec(`INSERT INTO runs(boot_id,observer_id,started_at,last_seen,stopped_at,release,upstream_revision,fingerprint,state) VALUES(?,?,?,?,?,'','','','stopped')`, fmt.Sprint("extra", i), "test", start.UnixMilli(), start.Add(time.Hour-131*time.Second).UnixMilli(), start.Add(time.Hour-131*time.Second).UnixMilli())
		if e != nil {
			t.Fatal(e)
		}
	}
	if _, e = tx.Exec("INSERT INTO association_hours(hour) VALUES(?)", start.Add(time.Hour).Unix()); e != nil {
		t.Fatal(e)
	}
	if e = tx.Commit(); e != nil {
		t.Fatal(e)
	}
	analyzeAll(t, s)
	page, e := s.Associations(context.Background(), Query{Start: start, End: start.Add(2 * time.Hour), Limit: 100})
	if e != nil || page.PendingHours != 0 || page.ProcessedHours != 2 || page.ExcludedHours["analysis_run_bound_exceeded"] != 1 {
		t.Fatal(page, e)
	}
}
func TestSchemaTwoUpgradeInvalidatesOldMethodAndKeepsReferences(t *testing.T) {
	dir := t.TempDir()
	db, e := sql.Open("sqlite3", filepath.Join(dir, "observer.sqlite"))
	if e != nil {
		t.Fatal(e)
	}
	for _, file := range []string{"001_initial.sql", "002_associations.sql"} {
		b, e := migrations.ReadFile("migrations/" + file)
		if e != nil {
			t.Fatal(e)
		}
		if _, e = db.Exec(string(b)); e != nil {
			t.Fatal(e)
		}
	}
	hour := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	_, e = db.Exec(`INSERT INTO runs(boot_id,observer_id,started_at,last_seen,stopped_at,release,upstream_revision,fingerprint,state) VALUES('old','test',?,?,?,?,?,?,'stopped')`, hour.UnixMilli(), hour.Add(2*time.Hour).UnixMilli(), hour.Add(2*time.Hour).UnixMilli(), "0.3.0", "", "")
	if e != nil {
		t.Fatal(e)
	}
	_, e = db.Exec(`INSERT INTO sessions(session_id,boot_id,peer_id,ip,port,client_name,capabilities,started_at,ready_at,start_mono,ended_at,end_mono) VALUES('s','old','peer','ip',1,'','[]',?,?,0,?,?)`, hour.UnixMilli(), hour.UnixMilli(), hour.Add(2*time.Hour).UnixMilli(), int64(2*time.Hour))
	if e != nil {
		t.Fatal(e)
	}
	part := analysis.Part{Hour: hour, Boot: "old", Cohort: "cohort", Producer: "p", Peer: "peer", Members: []string{"s"}, Kind: "hash_announcement", Counts: analysis.Counts{Eligible: 1, Received: 1}}
	raw, _ := json.Marshal(part)
	if _, e = db.Exec("INSERT INTO association_hours(hour,computed_revision,computed_at) VALUES(?,1,?); INSERT INTO association_parts(hour,cohort,producer,peer,kind,boot_id,payload) VALUES(?,'cohort','p','peer','hash_announcement','old',?)", hour.Unix(), hour.UnixMilli(), hour.Unix(), string(raw)); e != nil {
		t.Fatal(e)
	}
	db.Close()
	s, e := Open(dir, 2<<30, 7)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	var refs, version int
	s.read.QueryRow("SELECT count(*) FROM association_session_refs WHERE session_id='s'").Scan(&refs)
	s.read.QueryRow("PRAGMA user_version").Scan(&version)
	if refs != 1 || version != 3 {
		t.Fatal(refs, version)
	}
	page, e := s.Associations(context.Background(), Query{Start: hour, End: hour.Add(time.Hour), Limit: 100})
	if e != nil || len(page.Data) != 0 || page.PendingHours != 1 || page.Method != "association-2" {
		t.Fatal(page, e)
	}
	analyzeAll(t, s)
	s.read.QueryRow("SELECT count(*) FROM association_session_refs").Scan(&refs)
	if refs != 0 {
		t.Fatal("stale cohort references survived replay")
	}
}
func TestPartLimitWithEightPeersDoesNotBlockNextHour(t *testing.T) {
	s, e := Open(t.TempDir(), 2<<30, 7)
	if e != nil {
		t.Fatal(e)
	}
	defer s.Close()
	hour := time.Now().UTC().Truncate(time.Hour).Add(-4 * time.Hour)
	if e = s.WriteBatch(records(map[string]any{"type": "run_started", "observerId": "test", "bootId": "boot", "startedAt": hour}, map[string]any{"type": "run_status", "bootId": "boot", "at": hour.Add(3 * time.Hour)})); e != nil {
		t.Fatal(e)
	}
	for group := 0; group < 3; group++ {
		begin := time.Duration(group) * 20 * time.Minute
		end := hour.Add(begin + 20*time.Minute)
		for peer := 0; peer < 8; peer++ {
			ready := begin + 40*time.Second + time.Duration(peer)*time.Second
			ss := capture.Session{ID: fmt.Sprintf("s-%d-%d", group, peer), PeerID: strings.Repeat(fmt.Sprint(peer), 128), IP: fmt.Sprintf("8.8.8.%d", peer+1), Started: hour.Add(ready), Ready: hour.Add(ready), StartMono: int64(ready), Ended: &end, EndMono: int64(begin + 20*time.Minute)}
			if e = s.WriteBatch(records(map[string]any{"type": "session_ended", "bootId": "boot", "session": ss})); e != nil {
				t.Fatal(e)
			}
		}
		for producer := 0; producer < 86; producer++ {
			at := hour.Add(begin + 70*time.Second + time.Duration(producer)*10*time.Second)
			hash := types.NewHash([]byte(fmt.Sprintf("%d-%d", group, producer)))
			header := protocol.Header{Hash: hash, Height: uint64(group*100 + producer + 1), Producer: fmt.Sprint("producer-", producer), Timestamp: uint64(at.Unix()), Verification: "rpc_agreed"}
			for _, kind := range []string{"hash_announcement", "full_broadcast"} {
				a := capture.Arrival{Type: "arrival", ObserverID: "test", BootID: "boot", SessionID: fmt.Sprintf("s-%d-0", group), PeerID: strings.Repeat("0", 128), Hash: hash, Kind: kind, UTC: at, Mono: at.Sub(hour).Nanoseconds(), Eligible: true, Header: &header}
				if e = s.WriteBatch(records(a)); e != nil {
					t.Fatal(e)
				}
			}
		}
	}
	if _, e = s.write.Exec("INSERT INTO association_hours(hour) VALUES(?)", hour.Add(time.Hour).Unix()); e != nil {
		t.Fatal(e)
	}
	analyzeAll(t, s)
	page, e := s.Associations(context.Background(), Query{Start: hour, End: hour.Add(2 * time.Hour), Limit: 100})
	if e != nil || page.PendingHours != 0 || page.ProcessedHours != 2 || page.ExcludedHours["analysis_parts_bound_exceeded"] != 1 {
		t.Fatal(page, e)
	}
	q, e := s.Quality(context.Background(), hour, hour.Add(2*time.Hour))
	if e != nil || q["associationSummariesAvailable"] != false {
		t.Fatal(q, e)
	}
}
