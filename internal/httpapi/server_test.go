package httpapi

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/storage"
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
