package httpapi

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/storage"
	"github.com/zenon-network/go-zenon/common/types"
)

const MaxResponseBytes = 1024 * 1024
const MethodVersion = "collection-1"

type Snapshot struct {
	Report  map[string]any `json:"report"`
	Ready   bool           `json:"ready"`
	Updated time.Time      `json:"updatedAt"`
}
type Status struct{ value atomic.Pointer[[]byte] }

func (s *Status) Set(report map[string]any, ready bool) {
	b, _ := json.Marshal(Snapshot{report, ready, time.Now().UTC()})
	s.value.Store(&b)
}
func (s *Status) Get() Snapshot {
	var v Snapshot
	if b := s.value.Load(); b != nil {
		_ = json.Unmarshal(*b, &v)
	}
	if v.Report == nil {
		v.Report = map[string]any{"captureState": "initializing"}
	}
	if time.Since(v.Updated) > 30*time.Second {
		v.Ready = false
		v.Report["statusStale"] = true
	}
	return v
}

type API struct {
	Store     *storage.Store
	Status    *Status
	Observer  string
	Retention int
	origins   map[string]bool
	slots     chan struct{}
	mu        sync.Mutex
	tokens    float64
	last      time.Time
}

func New(store *storage.Store, status *Status, observer string, retention int, origins []string) *API {
	a := &API{Store: store, Status: status, Observer: observer, Retention: retention, origins: map[string]bool{}, slots: make(chan struct{}, 8), tokens: 40, last: time.Now()}
	for _, o := range origins {
		a.origins[o] = true
	}
	return a
}
func ValidOrigin(o string) bool {
	u, e := url.Parse(o)
	return e == nil && (u.Scheme == "https" || u.Scheme == "http") && u.Host != "" && u.User == nil && u.Path == "" && u.RawQuery == "" && u.Fragment == "" && !strings.Contains(o, "*")
}
func (a *API) allowed() bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	now := time.Now()
	a.tokens = min(40, a.tokens+now.Sub(a.last).Seconds()*20)
	a.last = now
	if a.tokens < 1 {
		return false
	}
	a.tokens--
	return true
}
func (a *API) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Add("Vary", "Origin")
	if len(r.RequestURI) > 4096 {
		a.failure(w, r, 414, "request_uri_too_long")
		return
	}
	if r.ContentLength > 0 || len(r.TransferEncoding) > 0 {
		a.failure(w, r, 413, "request_body_not_allowed")
		return
	}
	origin := r.Header.Get("Origin")
	if origin != "" && a.origins[origin] {
		w.Header().Set("Access-Control-Allow-Origin", origin)
		w.Header().Set("Access-Control-Allow-Methods", "GET, HEAD, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "If-None-Match")
		w.Header().Set("Access-Control-Expose-Headers", "ETag")
	}
	if r.Method == "OPTIONS" {
		if origin != "" && !a.origins[origin] {
			a.failure(w, r, 403, "origin_not_allowed")
			return
		}
		w.WriteHeader(204)
		return
	}
	if r.Method != "GET" && r.Method != "HEAD" {
		w.Header().Set("Allow", "GET, HEAD, OPTIONS")
		a.failure(w, r, 405, "method_not_allowed")
		return
	}
	if r.URL.Path == "/healthz" {
		a.respond(w, r, 200, map[string]any{"status": "alive"}, false)
		return
	}
	if r.URL.Path == "/readyz" {
		state := a.Status.Get()
		code := 200
		if !state.Ready {
			code = 503
		}
		a.respond(w, r, code, map[string]any{"ready": state.Ready, "captureState": state.Report["captureState"]}, false)
		return
	}
	if !a.allowed() {
		w.Header().Set("Retry-After", "1")
		a.failure(w, r, 429, "rate_limit")
		return
	}
	select {
	case a.slots <- struct{}{}:
		defer func() { <-a.slots }()
	default:
		a.failure(w, r, 503, "query_capacity")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 3*time.Second)
	defer cancel()
	p, err := a.parse(r)
	if err != nil {
		a.failure(w, r, 400, err.Error())
		return
	}
	var payload any
	resource := ""
	path := r.URL.Path
	switch {
	case path == "/api/v1/status":
		snap := a.Status.Get()
		payload = snap.Report
	case path == "/api/v1/peers":
		resource = "peers"
	case path == "/api/v1/momentums":
		resource = "momentums"
	case strings.HasPrefix(path, "/api/v1/momentums/") && strings.HasSuffix(path, "/arrivals"):
		p.Hash = strings.TrimSuffix(strings.TrimPrefix(path, "/api/v1/momentums/"), "/arrivals")
		if h, e := types.HexToHash(p.Hash); e != nil || h.IsZero() || len(p.Hash) != 64 {
			a.failure(w, r, 400, "invalid_hash")
			return
		}
		resource = "arrivals"
	case path == "/api/v1/associations":
		payload = map[string]any{"data": []any{}, "nextOffset": nil, "available": false, "reason": "association_analysis_not_implemented", "producerSoftwareVersion": nil}
	case strings.HasPrefix(path, "/api/v1/pillars/"):
		p.Producer = strings.TrimPrefix(path, "/api/v1/pillars/")
		if _, e := types.ParseAddress(p.Producer); e != nil {
			a.failure(w, r, 400, "invalid_producer_address")
			return
		}
		resource = "pillar"
	case path == "/":
		payload = map[string]any{"service": "Zenon P2P Observer", "status": "/api/v1/status", "documentation": "https://github.com/0x3639/go-zenon-pillar-version/blob/main/docs/api.md"}
	default:
		a.failure(w, r, 404, "not_found")
		return
	}
	if resource != "" {
		page, e := a.Store.Collection(ctx, resource, p)
		if e != nil {
			a.failure(w, r, 503, "storage_query_unavailable")
			return
		}
		payload = page
		if resource == "pillar" {
			momentums, e := a.Store.Collection(ctx, "momentums", p)
			if e != nil {
				a.failure(w, r, 503, "storage_query_unavailable")
				return
			}
			payload = map[string]any{"producerAddress": p.Producer, "producerSoftwareVersion": nil, "nameSnapshots": page.Data, "nameSnapshotsNextOffset": page.NextOffset, "momentums": momentums.Data, "momentumsNextOffset": momentums.NextOffset, "associations": []any{}, "identityAssertions": []any{}, "associationAnalysisAvailable": false}
		}
	}
	quality, e := a.Store.Quality(ctx, p.Start, p.End)
	if e != nil {
		a.failure(w, r, 503, "storage_query_unavailable")
		return
	}
	snapshot := a.Status.Get()
	quality["captureState"] = snapshot.Report["captureState"]
	quality["statusUpdatedAt"] = snapshot.Updated
	quality["ready"] = snapshot.Ready
	body := map[string]any{"schemaVersion": "1.0", "observerId": a.Observer, "generatedAt": snapshot.Updated, "windowStart": p.Start, "windowEnd": p.End, "methodVersion": MethodVersion, "dataQuality": quality, "result": payload}
	a.respond(w, r, 200, body, path != "/api/v1/status")
}
func (a *API) parse(r *http.Request) (storage.Query, error) {
	// Stable 30-second boundaries make the default window cacheable.
	now := time.Now().UTC()
	end := now.Truncate(30 * time.Second)
	p := storage.Query{Start: end.Add(-24 * time.Hour), End: end, Limit: 100}
	q := r.URL.Query()
	for k, v := range q {
		if len(v) != 1 {
			return p, fmt.Errorf("duplicate_parameter")
		}
		switch k {
		case "from", "to", "limit", "offset", "producer", "peerId", "eventKind":
		default:
			return p, fmt.Errorf("unknown_parameter")
		}
	}
	var err error
	if v := q.Get("to"); v != "" {
		p.End, err = time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return p, fmt.Errorf("invalid_to")
		}
		p.Start = p.End.Add(-24 * time.Hour)
	}
	if v := q.Get("from"); v != "" {
		p.Start, err = time.Parse(time.RFC3339Nano, v)
		if err != nil {
			return p, fmt.Errorf("invalid_from")
		}
	}
	if !p.End.After(p.Start) || p.End.Sub(p.Start) > 24*time.Hour || p.End.After(now) || p.Start.Before(now.Add(-time.Duration(a.Retention)*24*time.Hour-30*time.Second)) {
		return p, fmt.Errorf("range_must_be_within_retained_history_and_at_most_24_hours")
	}
	if v := q.Get("limit"); v != "" {
		p.Limit, err = strconv.Atoi(v)
		if err != nil || p.Limit < 1 || p.Limit > 500 {
			return p, fmt.Errorf("limit_must_be_1_to_500")
		}
	}
	if v := q.Get("offset"); v != "" {
		p.Offset, err = strconv.Atoi(v)
		if err != nil || p.Offset < 0 || p.Offset > 100000 {
			return p, fmt.Errorf("offset_must_be_0_to_100000")
		}
	}
	p.Producer = q.Get("producer")
	if p.Producer != "" {
		if _, e := types.ParseAddress(p.Producer); e != nil {
			return p, fmt.Errorf("invalid_producer_address")
		}
	}
	p.Peer = q.Get("peerId")
	if p.Peer != "" {
		if len(p.Peer) != 128 || strings.Trim(p.Peer, "0123456789abcdef") != "" {
			return p, fmt.Errorf("invalid_peer_id")
		}
	}
	p.Kind = q.Get("eventKind")
	if p.Kind != "" && p.Kind != "hash_announcement" && p.Kind != "full_broadcast" && p.Kind != "requested_response" && p.Kind != "historical_or_unresolved" {
		return p, fmt.Errorf("invalid_event_kind")
	}
	p.Start = p.Start.UTC()
	p.End = p.End.UTC()
	return p, nil
}
func (a *API) failure(w http.ResponseWriter, r *http.Request, code int, message string) {
	a.respond(w, r, code, map[string]any{"error": message}, false)
}
func (a *API) respond(w http.ResponseWriter, r *http.Request, code int, v any, cache bool) {
	b, e := json.Marshal(v)
	if e != nil {
		code = 500
		b = []byte(`{"error":"encoding_failed"}`)
	}
	if len(b) > MaxResponseBytes {
		code = 413
		b = []byte(`{"error":"response_limit_reduce_page_size"}`)
		cache = false
	}
	if cache && code == 200 {
		w.Header().Set("Cache-Control", "public, max-age=30")
		tag := fmt.Sprintf(`"%x"`, sha256.Sum256(b))
		w.Header().Set("ETag", tag)
		for _, candidate := range strings.Split(r.Header.Get("If-None-Match"), ",") {
			if strings.TrimSpace(candidate) == tag || strings.TrimSpace(candidate) == "*" {
				w.WriteHeader(304)
				return
			}
		}
	}
	w.Header().Set("Content-Length", strconv.Itoa(len(b)))
	w.WriteHeader(code)
	if r.Method != "HEAD" {
		_, _ = w.Write(b)
	}
}

type boundedListener struct {
	net.Listener
	slots chan struct{}
	done  chan struct{}
	once  sync.Once
}
type boundedConn struct {
	net.Conn
	release func()
	once    sync.Once
}

func (c *boundedConn) Close() error { e := c.Conn.Close(); c.once.Do(c.release); return e }
func (l *boundedListener) Accept() (net.Conn, error) {
	select {
	case l.slots <- struct{}{}:
	case <-l.done:
		return nil, net.ErrClosed
	}
	c, e := l.Listener.Accept()
	if e != nil {
		<-l.slots
		return nil, e
	}
	return &boundedConn{Conn: c, release: func() { <-l.slots }}, nil
}
func (l *boundedListener) Close() error {
	l.once.Do(func() { close(l.done) })
	return l.Listener.Close()
}
func Listen(port int, handler http.Handler) (*http.Server, net.Listener, error) {
	l, e := net.Listen("tcp", net.JoinHostPort("0.0.0.0", strconv.Itoa(port)))
	if e != nil {
		return nil, nil, e
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 * 1024}
	return srv, &boundedListener{Listener: l, slots: make(chan struct{}, 64), done: make(chan struct{})}, nil
}
