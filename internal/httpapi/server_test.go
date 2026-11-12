package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/0x3639/go-zenon-pillar-version/internal/storage"
	"github.com/zenon-network/go-zenon/common/types"
)

func testAPI(t *testing.T) *API {
	t.Helper()
	s, e := storage.Open(t.TempDir(), 2<<30, 7)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(s.Close)
	state := &Status{}
	state.Set(map[string]any{"captureState": "observing"}, true)
	return New(s, state, "test", 7, []string{"https://0x3639.com"})
}
func TestReadOnlyBoundsAndCORS(t *testing.T) {
	a := testAPI(t)
	cases := []struct {
		method, path, origin string
		code                 int
	}{
		{"GET", "/healthz", "", 200}, {"HEAD", "/api/v1/peers", "", 200}, {"POST", "/api/v1/peers", "", 405},
		{"GET", "/api/v1/momentums?limit=501", "", 400}, {"GET", "/api/v1/peers?offset=100001", "", 400},
		{"GET", "/api/v1/peers?from=2020-01-01T00:00:00Z", "", 400}, {"GET", "/api/v1/peers?to=2099-01-01T00:00:00Z", "", 400},
		{"GET", "/api/v1/peers?url=http://127.0.0.1", "", 400}, {"GET", "/api/v1/peers?limit=1&limit=2", "", 400},
		{"GET", "/api/v1/momentums/notahash/arrivals", "", 400}, {"GET", "/api/v1/pillars/invalid", "", 400},
		{"OPTIONS", "/api/v1/peers", "https://0x3639.com", 204}, {"OPTIONS", "/api/v1/peers", "https://evil.example", 403},
		{"GET", "/api/v1/status", "https://0x3639.com", 200}, {"GET", "/no-such-route", "", 404},
	}
	for _, c := range cases {
		r := httptest.NewRequest(c.method, c.path, nil)
		r.Header.Set("Origin", c.origin)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, r)
		if w.Code != c.code {
			t.Fatalf("%s %s: %d %s", c.method, c.path, w.Code, w.Body.String())
		}
		if c.method == "HEAD" && w.Body.Len() != 0 {
			t.Fatal("HEAD body")
		}
		if c.origin == "https://0x3639.com" && w.Header().Get("Access-Control-Allow-Origin") != c.origin {
			t.Fatal("allowed origin missing")
		}
	}
	r := httptest.NewRequest("GET", "/api/v1/status", strings.NewReader("unexpected body"))
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != 413 {
		t.Fatal(w.Code)
	}
}
func TestETagUnknownAttributionAndReadiness(t *testing.T) {
	a := testAPI(t)
	r := httptest.NewRequest("GET", "/api/v1/associations", nil)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, r)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"producerSoftwareVersion":null`) || !strings.Contains(w.Body.String(), `"available":false`) {
		t.Fatal(w.Body.String())
	}
	r.Header.Set("If-None-Match", w.Header().Get("ETag"))
	again := httptest.NewRecorder()
	a.ServeHTTP(again, r)
	if again.Code != 304 {
		t.Fatal(again.Code, again.Body.String())
	}
	a.Status.Set(map[string]any{"captureState": "storage_paused"}, false)
	for path, expected := range map[string]int{"/healthz": 200, "/readyz": 503, "/api/v1/momentums": 200} {
		w = httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != expected {
			t.Fatal(path, w.Code)
		}
	}
	stale, _ := json.Marshal(Snapshot{Report: map[string]any{"captureState": "observing"}, Ready: true, Updated: time.Now().Add(-time.Minute)})
	a.Status.value.Store(&stale)
	if a.Status.Get().Ready {
		t.Fatal("stale status ready")
	}
}
func TestResponseLimitAndDatabaseCancellation(t *testing.T) {
	a := testAPI(t)
	w := httptest.NewRecorder()
	a.respond(w, httptest.NewRequest("GET", "/", nil), 200, strings.Repeat("a", MaxResponseBytes+1), true)
	if w.Code != 413 {
		t.Fatal(w.Code)
	}
	ctx, end := context.WithCancel(context.Background())
	end()
	if e := a.Store.Ping(ctx); e == nil {
		t.Fatal("canceled query succeeded")
	}
	if ValidOrigin("https://0x3639.com/go-zenon-version/") || ValidOrigin("*") || !ValidOrigin("https://0x3639.com") {
		t.Fatal("origin validation")
	}
}

func TestWindowDatesAreNormalizedToUTC(t *testing.T) {
	a := testAPI(t)
	end := time.Now().Add(-time.Minute).In(time.FixedZone("offset", -6*3600))
	q := url.Values{"from": {end.Add(-time.Hour).Format(time.RFC3339)}, "to": {end.Format(time.RFC3339)}}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/momentums?"+q.Encode(), nil))
	if w.Code != 200 {
		t.Fatal(w.Body.String())
	}
	var v map[string]any
	json.Unmarshal(w.Body.Bytes(), &v)
	if !strings.HasSuffix(v["windowStart"].(string), "Z") || !strings.HasSuffix(v["windowEnd"].(string), "Z") {
		t.Fatal(v)
	}
}

func TestAssociationWindowsAndExclusions(t *testing.T) {
	a := testAPI(t)
	end := time.Now().UTC().Add(-2 * time.Hour).Truncate(time.Hour)
	for _, c := range []struct {
		days int
		kind string
		code int
	}{{3, "hash_announcement", 200}, {30, "full_broadcast", 200}, {91, "", 400}, {3, "requested_response", 400}} {
		q := url.Values{"from": {end.Add(-time.Duration(c.days) * 24 * time.Hour).Format(time.RFC3339)}, "to": {end.Format(time.RFC3339)}, "eventKind": {c.kind}}
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/associations?"+q.Encode(), nil))
		if w.Code != c.code {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	q := url.Values{"from": {end.Add(-time.Hour + time.Second).Format(time.RFC3339)}, "to": {end.Format(time.RFC3339)}}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/associations?"+q.Encode(), nil))
	if w.Code != 400 {
		t.Fatal(w.Body.String())
	}
}

func TestComputedAssociationEndpointAndPillar(t *testing.T) {
	a := testAPI(t)
	hour := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	events := []any{map[string]any{"type": "run_started", "observerId": "test", "bootId": "boot", "startedAt": hour}, map[string]any{"type": "run_status", "bootId": "boot", "at": hour.Add(2 * time.Hour)}}
	for _, id := range []string{"a", "b"} {
		events = append(events, map[string]any{"type": "session_started", "bootId": "boot", "session": capture.Session{ID: id, PeerID: strings.Repeat(id, 128), IP: id, Started: hour, Ready: hour}})
	}
	producer := "z1qqjxm297wcxhuqlqtcrjh8x3e7e6e0gtfj4cq4"
	h := protocol.Header{Hash: types.NewHash([]byte("http-sample")), Producer: producer, Height: 1, Timestamp: uint64(hour.Add(time.Minute).Unix()), Verification: "rpc_agreed"}
	events = append(events, capture.Arrival{Type: "arrival", ObserverID: "test", BootID: "boot", SessionID: "a", PeerID: strings.Repeat("a", 128), Hash: h.Hash, Kind: "hash_announcement", UTC: hour.Add(time.Minute), Mono: int64(time.Minute), Eligible: true, Header: &h})
	batch := [][]byte{}
	for _, event := range events {
		b, _ := json.Marshal(event)
		batch = append(batch, b)
	}
	if e := a.Store.WriteBatch(batch); e != nil {
		t.Fatal(e)
	}
	if _, e := a.Store.AnalyzeNext(context.Background(), time.Now()); e != nil {
		t.Fatal(e)
	}
	q := url.Values{"from": {hour.Format(time.RFC3339)}, "to": {hour.Add(time.Hour).Format(time.RFC3339)}}
	for _, path := range []string{"/api/v1/associations", "/api/v1/pillars/" + producer} {
		w := httptest.NewRecorder()
		a.ServeHTTP(w, httptest.NewRequest("GET", path+"?"+q.Encode(), nil))
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"producerSoftwareVersion":null`) || !strings.Contains(w.Body.String(), `"eligibleCount":1`) || !strings.Contains(w.Body.String(), `"cohortId"`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	q.Set("peerId", strings.Repeat("b", 128))
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/associations?"+q.Encode(), nil))
	if !strings.Contains(w.Body.String(), `"receivedCount":0`) {
		t.Fatal(w.Body.String())
	}
}

func TestStatusDoesNotClaimRankingsBeforeAnalysis(t *testing.T) {
	a := testAPI(t)
	a.Status.Set(map[string]any{"captureState": "observing", "rankingAvailable": true}, true)
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/status", nil))
	var envelope struct {
		Result struct {
			Available bool `json:"rankingAvailable"`
		}
	}
	if e := json.Unmarshal(w.Body.Bytes(), &envelope); e != nil {
		t.Fatal(e)
	}
	if w.Code != 200 || envelope.Result.Available {
		t.Fatal(w.Body.String())
	}
}

func TestPillarWithoutCompleteHourHasNullAssociationBounds(t *testing.T) {
	a := testAPI(t)
	h := time.Now().UTC().Truncate(time.Hour).Add(-3 * time.Hour)
	q := url.Values{"from": {h.Add(15 * time.Minute).Format(time.RFC3339)}, "to": {h.Add(45 * time.Minute).Format(time.RFC3339)}}
	w := httptest.NewRecorder()
	a.ServeHTTP(w, httptest.NewRequest("GET", "/api/v1/pillars/z1qqjxm297wcxhuqlqtcrjh8x3e7e6e0gtfj4cq4?"+q.Encode(), nil))
	var got struct {
		Result struct {
			Start   *time.Time     `json:"associationWindowStart"`
			End     *time.Time     `json:"associationWindowEnd"`
			Summary map[string]any `json:"associationSummary"`
		}
	}
	if e := json.Unmarshal(w.Body.Bytes(), &got); e != nil {
		t.Fatal(e)
	}
	if w.Code != 200 || got.Result.Start != nil || got.Result.End != nil || got.Result.Summary["reason"] != "no_complete_hours" {
		t.Fatal(w.Body.String())
	}
}
