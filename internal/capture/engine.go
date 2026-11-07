package capture

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/zenon-network/go-zenon/common/types"
)

const MaxArrivals = 20000
const MaxHeaders = 2048

type Session struct {
	ID           string          `json:"sessionId"`
	PeerID       string          `json:"peerId"`
	IP           string          `json:"ip"`
	Port         uint16          `json:"port"`
	ClientName   string          `json:"clientName"`
	Version      *string         `json:"advertisedVersion"`
	Capabilities []string        `json:"capabilities"`
	Started      time.Time       `json:"connectedAt"`
	Ready        time.Time       `json:"statusCompletedAt"`
	StartMono    int64           `json:"statusCompletedMonoNs"`
	Ended        *time.Time      `json:"disconnectedAt"`
	EndMono      int64           `json:"disconnectedMonoNs"`
	Reason       string          `json:"disconnectReason"`
	Reference    protocol.Header `json:"statusReference"`
}
type Arrival struct {
	Type       string           `json:"type"`
	ObserverID string           `json:"observerId"`
	BootID     string           `json:"bootId"`
	SessionID  string           `json:"sessionId"`
	PeerID     string           `json:"peerId"`
	Hash       types.Hash       `json:"hash"`
	Kind       string           `json:"eventKind"`
	UTC        time.Time        `json:"arrivedAt"`
	Mono       int64            `json:"monotonicNs"`
	Duplicates uint16           `json:"duplicateCount"`
	Header     *protocol.Header `json:"header"`
	Eligible   bool             `json:"eligible"`
	Exclusion  string           `json:"exclusionReason"`
}
type Engine struct {
	mu                 sync.Mutex
	ObserverID, BootID string
	Start              time.Time
	Writer             *Writer
	sessions           map[string]*Session
	arrivals           map[string]*Arrival
	headers            map[types.Hash]protocol.Header
	order              []types.Hash
	Resolve            chan types.Hash
	pending            map[types.Hash]bool
	ClockJumps         int
	clockBadUntil      time.Time
	lastClock          time.Time
	gap                bool
	LimitReached       bool
}

func NewEngine(observer, boot string, w *Writer) *Engine {
	now := time.Now()
	return &Engine{ObserverID: observer, BootID: boot, Start: now, Writer: w, sessions: map[string]*Session{}, arrivals: map[string]*Arrival{}, headers: map[types.Hash]protocol.Header{}, Resolve: make(chan types.Hash, 128), pending: map[types.Hash]bool{}, lastClock: now}
}
func (e *Engine) AddSession(s Session) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s.StartMono = s.Ready.Sub(e.Start).Nanoseconds()
	e.sessions[s.ID] = &s
	e.Writer.Submit(map[string]any{"type": "session_started", "bootId": e.BootID, "session": s})
}
func (e *Engine) EndSession(id, reason string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if s := e.sessions[id]; s != nil {
		now := time.Now()
		s.Ended = &now
		s.EndMono = now.Sub(e.Start).Nanoseconds()
		s.Reason = reason
		e.Writer.Submit(map[string]any{"type": "session_ended", "bootId": e.BootID, "session": s})
	}
}
func (e *Engine) eligibility(a *Arrival, s *Session) {
	a.Eligible = false
	switch {
	case a.Kind == "requested_response":
		a.Exclusion = "requested_response"
	case a.Kind == "historical_or_unresolved":
		a.Exclusion = "unmatched_response"
	case a.Header == nil:
		a.Exclusion = "unresolved"
	case e.gap:
		a.Exclusion = "capture_gap"
	case a.UTC.Before(e.clockBadUntil):
		a.Exclusion = "clock_discontinuity"
	default:
		a.Exclusion = protocol.Exclusion(*a.Header, a.UTC, s.Ready)
	}
	a.Eligible = a.Exclusion == ""
}
func (e *Engine) Observe(session string, hash types.Hash, kind string, at time.Time, h *protocol.Header) {
	e.mu.Lock()
	defer e.mu.Unlock()
	s := e.sessions[session]
	if s == nil {
		return
	}
	key := session + ":" + hash.String() + ":" + kind
	if a := e.arrivals[key]; a != nil {
		if a.Duplicates < 65535 {
			a.Duplicates++
		}
		return
	}
	if len(e.arrivals) >= MaxArrivals {
		e.LimitReached = true
		e.Writer.Drops.Add(1)
		e.gap = true
		return
	}
	a := &Arrival{Type: "arrival", ObserverID: e.ObserverID, BootID: e.BootID, SessionID: session, PeerID: s.PeerID, Hash: hash, Kind: kind, UTC: at.UTC(), Mono: at.Sub(e.Start).Nanoseconds()}
	if h != nil {
		copy := *h
		a.Header = &copy
		e.putHeader(*h)
	} else if cached, ok := e.headers[hash]; ok {
		a.Header = &cached
	}
	e.eligibility(a, s)
	e.arrivals[key] = a
	if !e.Writer.Submit(a) {
		e.gap = true
	}
	if a.Header == nil && kind == "hash_announcement" && !e.pending[hash] {
		select {
		case e.Resolve <- hash:
			e.pending[hash] = true
		default:
			e.Writer.Drops.Add(1)
			e.gap = true
		}
	}
}
func (e *Engine) putHeader(h protocol.Header) {
	if _, ok := e.headers[h.Hash]; !ok {
		if len(e.order) >= MaxHeaders {
			delete(e.headers, e.order[0])
			e.order = e.order[1:]
		}
		e.order = append(e.order, h.Hash)
	}
	e.headers[h.Hash] = h
}
func (e *Engine) Resolved(h protocol.Header) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.putHeader(h)
	delete(e.pending, h.Hash)
	for _, a := range e.arrivals {
		if a.Hash == h.Hash && a.Header == nil {
			copy := h
			a.Header = &copy
			e.eligibility(a, e.sessions[a.SessionID])
			update := *a
			update.Type = "arrival_resolved"
			if !e.Writer.Submit(update) {
				e.gap = true
			}
		}
	}
}
func (e *Engine) ResolveFailed(hash types.Hash) {
	e.mu.Lock()
	defer e.mu.Unlock()
	delete(e.pending, hash)
}
func (e *Engine) Tick(now time.Time) {
	e.mu.Lock()
	defer e.mu.Unlock()
	wall := now.UnixNano() - e.lastClock.UnixNano()
	mono := now.Sub(e.lastClock).Nanoseconds()
	diff := wall - mono
	if diff < 0 {
		diff = -diff
	}
	if diff > int64(100*time.Millisecond) {
		e.ClockJumps++
		e.clockBadUntil = now.Add(time.Minute)
		e.gap = true
		e.Writer.Submit(map[string]any{"type": "clock_discontinuity", "at": now.UTC(), "differenceNs": diff})
	}
	e.lastClock = now
	if e.Writer.Drops.Load() > 0 {
		e.gap = true
	}
}
func (e *Engine) Report(now time.Time) map[string]any {
	e.mu.Lock()
	defer e.mu.Unlock()
	checked := map[types.Hash]bool{}
	multi := map[types.Hash]map[string]bool{}
	byKind := map[string]int{}
	excluded := map[string]int{}
	duplicates := uint64(0)
	for _, a := range e.arrivals {
		byKind[a.Kind]++
		duplicates += uint64(a.Duplicates)
		if a.Eligible {
			checked[a.Hash] = true
			if multi[a.Hash] == nil {
				multi[a.Hash] = map[string]bool{}
			}
			multi[a.Hash][a.PeerID] = true
		} else {
			excluded[a.Exclusion]++
		}
	}
	two := 0
	for _, peers := range multi {
		if len(peers) >= 2 {
			two++
		}
	}
	sessions := make([]Session, 0, len(e.sessions))
	for _, s := range e.sessions {
		sessions = append(sessions, *s)
	}
	sort.Slice(sessions, func(i, j int) bool { return sessions[i].StartMono < sessions[j].StartMono })
	maxOverlap := int64(0)
	for i, a := range sessions {
		for _, b := range sessions[i+1:] {
			if a.PeerID == b.PeerID || a.IP == b.IP {
				continue
			}
			start := max(a.StartMono, b.StartMono)
			endA, endB := a.EndMono, b.EndMono
			if endA == 0 {
				endA = now.Sub(e.Start).Nanoseconds()
			}
			if endB == 0 {
				endB = now.Sub(e.Start).Nanoseconds()
			}
			overlap := min(endA, endB) - start
			if overlap > maxOverlap {
				maxOverlap = overlap
			}
		}
	}
	passed := maxOverlap >= int64(time.Hour) && two >= 100 && e.Writer.Drops.Load() == 0 && !e.Writer.Failed.Load() && e.ClockJumps == 0
	return map[string]any{"schemaVersion": "1.0", "observerId": e.ObserverID, "bootId": e.BootID, "generatedAt": now.UTC(), "windowStart": e.Start.UTC(), "methodVersion": "feasibility-1", "milestone1Passed": passed, "distinctCheckedLiveMomentums": len(checked), "distinctCheckedLiveMomentumsSeenFromTwoPeers": two, "maxTwoSessionOverlapSeconds": float64(maxOverlap) / 1e9, "sessions": sessions, "arrivalsByKind": byKind, "excludedByReason": excluded, "duplicateCount": duplicates, "queueDepth": e.Writer.QueueDepth(), "captureDrops": e.Writer.Drops.Load(), "clockJumps": e.ClockJumps, "evidenceBytes": e.Writer.Bytes.Load(), "storageError": e.Writer.Error(), "arrivalLimitReached": e.LimitReached, "rankingAvailable": false, "producerSoftwareVersion": nil}
}
func (e *Engine) Stopped() bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.LimitReached || e.Writer.Failed.Load()
}
func (e *Engine) FinalDuplicates() {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, a := range e.arrivals {
		if a.Duplicates > 0 {
			e.Writer.Submit(map[string]any{"type": "duplicate_summary", "sessionId": a.SessionID, "hash": a.Hash, "eventKind": a.Kind, "duplicateCount": a.Duplicates})
		}
	}
}
func (e *Engine) SessionID(n uint64) string { return fmt.Sprintf("%s-%d", e.BootID, n) }
