package limits

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var ErrTraffic = errors.New("traffic_paused")

type Bucket struct {
	Minute  int64 `json:"minute"`
	In, Out uint64
}
type Cooldown struct {
	Failures int       `json:"failures"`
	Until    time.Time `json:"until"`
}
type State struct {
	Buckets     []Bucket            `json:"buckets"`
	Cooldowns   map[string]Cooldown `json:"cooldowns"`
	PausedUntil time.Time           `json:"pausedUntil"`
	LastSaved   time.Time           `json:"lastSaved"`
}
type Budget struct {
	mu                  sync.Mutex
	path                string
	state               State
	inLimit, outLimit   uint64
	creditIn, creditOut uint64
	creditMinute        int64
	actualIn, actualOut uint64
	persistError        error
	writeState          func(string, []byte) error
}

// AtomicJSON fsyncs data and directory before acknowledging a state change.
func AtomicJSON(path string, v any) error {
	b, e := json.MarshalIndent(v, "", "  ")
	if e != nil {
		return e
	}
	return atomicBytes(path, b)
}

func atomicBytes(path string, b []byte) error {
	f, e := os.OpenFile(path+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if e != nil {
		return e
	}
	if _, e = f.Write(b); e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return e
	}
	if closeErr != nil {
		return closeErr
	}
	if e = os.Rename(path+".tmp", path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func Open(path string, inLimit, outLimit uint64) (*Budget, error) {
	b := &Budget{path: path, inLimit: inLimit, outLimit: outLimit, state: State{Cooldowns: map[string]Cooldown{}}}
	var data []byte
	f, err := os.Open(path)
	if err == nil {
		data, err = io.ReadAll(io.LimitReader(f, 128*1024+1))
		_ = f.Close()
	}
	if err == nil {
		if len(data) > 128*1024 {
			return nil, errors.New("oversized budget state")
		}
		if err = json.Unmarshal(data, &b.state); err != nil {
			return nil, err
		}
		if len(b.state.Buckets) > 61 || len(b.state.Cooldowns) > 64 {
			return nil, errors.New("oversized budget collections")
		}
		if b.state.Cooldowns == nil {
			b.state.Cooldowns = map[string]Cooldown{}
		}
	} else if !os.IsNotExist(err) {
		return nil, err
	}
	if b.state.LastSaved.After(time.Now().Add(time.Second)) {
		return nil, errors.New("wall clock precedes persisted limits; wait for clock recovery")
	}
	return b, nil
}
func (b *Budget) prune(now time.Time) {
	min := now.Unix() / 60
	keep := b.state.Buckets[:0]
	for _, v := range b.state.Buckets {
		if v.Minute >= min-60 {
			keep = append(keep, v)
		}
	}
	b.state.Buckets = keep
	if b.creditMinute != min {
		b.creditIn = 0
		b.creditOut = 0
		b.creditMinute = min
	}
}
func (b *Budget) totals() (uint64, uint64) {
	var in, out uint64
	for _, v := range b.state.Buckets {
		in += v.In
		out += v.Out
	}
	return in, out
}
func (b *Budget) persist(now time.Time) error {
	if b.persistError != nil {
		return b.persistError
	}
	b.state.LastSaved = now
	data, err := json.MarshalIndent(b.state, "", "  ")
	if err != nil {
		return err
	}
	write := b.writeState
	if write == nil {
		write = atomicBytes
	}
	done := make(chan error, 1)
	// At most one persistence task can remain in flight: after a timeout,
	// the budget fails closed and never starts another write or grants credit.
	go func() { done <- write(b.path, data) }()
	timer := time.NewTimer(250 * time.Millisecond)
	defer timer.Stop()
	select {
	case err = <-done:
	case <-timer.C:
		err = errors.New("storage_paused: budget persistence deadline")
	}
	if err != nil {
		b.persistError = err
	}
	return err
}

// Reserve before socket I/O. Persistent reservations survive abrupt restarts.
// Unused credit is deliberately not refunded, so a restart cannot reset limits.
func (b *Budget) reserve(n uint64, incoming bool) error {
	_, err := b.reserveIO(n, incoming)
	return err
}

func (b *Budget) reserveIO(n uint64, incoming bool) (int64, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	now := time.Now()
	if b.persistError != nil {
		return 0, b.persistError
	}
	b.prune(now)
	if now.Before(b.state.PausedUntil) {
		return 0, ErrTraffic
	}
	credit := &b.creditOut
	if incoming {
		credit = &b.creditIn
	}
	if *credit < n {
		amount := uint64(64 * 1024)
		if n > amount {
			amount = n
		}
		in, out := b.totals()
		if (incoming && in+amount > b.inLimit) || (!incoming && out+amount > b.outLimit) {
			b.state.PausedUntil = now.Add(61 * time.Minute)
			if e := b.persist(now); e != nil {
				return 0, e
			}
			return 0, ErrTraffic
		}
		minute := now.Unix() / 60
		l := len(b.state.Buckets)
		if l == 0 || b.state.Buckets[l-1].Minute != minute {
			b.state.Buckets = append(b.state.Buckets, Bucket{Minute: minute})
			l++
		}
		if incoming {
			b.state.Buckets[l-1].In += amount
		} else {
			b.state.Buckets[l-1].Out += amount
		}
		if e := b.persist(now); e != nil {
			return 0, e
		}
		*credit += amount
	}
	*credit -= n
	return b.creditMinute, nil
}
func (b *Budget) refund(unused uint64, incoming bool, actual uint64, minute int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if incoming {
		if minute == b.creditMinute {
			b.creditIn += unused
		}
		b.actualIn += actual
	} else {
		if minute == b.creditMinute {
			b.creditOut += unused
		}
		b.actualOut += actual
	}
}
func (b *Budget) Snapshot() map[string]any {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.prune(time.Now())
	in, out := b.totals()
	var persistenceError any
	if b.persistError != nil {
		persistenceError = b.persistError.Error()
	}
	return map[string]any{"reservedIngressBytesRollingHour": in, "reservedEgressBytesRollingHour": out, "actualIngressBytesThisBoot": b.actualIn, "actualEgressBytesThisBoot": b.actualOut, "pausedUntil": b.state.PausedUntil, "ingressLimit": b.inLimit, "egressLimit": b.outLimit, "persistenceError": persistenceError}
}

func (b *Budget) Error() error { b.mu.Lock(); defer b.mu.Unlock(); return b.persistError }
func (b *Budget) Paused() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return time.Now().Before(b.state.PausedUntil)
}
func (b *Budget) Cooldown(id string) Cooldown {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.Cooldowns[id]
}
func (b *Budget) SetCooldown(id string, c Cooldown) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.state.Cooldowns[id]; !ok && len(b.state.Cooldowns) >= 64 {
		return errors.New("cooldown identity limit")
	}
	b.state.Cooldowns[id] = c
	return b.persist(time.Now())
}
func (b *Budget) Wrap(conn net.Conn) net.Conn { return &counted{Conn: conn, b: b} }

type counted struct {
	net.Conn
	b *Budget
}

func (c *counted) Read(p []byte) (int, error) {
	if len(p) > 32*1024 {
		p = p[:32*1024]
	}
	minute, err := c.b.reserveIO(uint64(len(p)), true)
	if err != nil {
		return 0, err
	}
	n, e := c.Conn.Read(p)
	c.b.refund(uint64(len(p)-n), true, uint64(n), minute)
	return n, e
}
func (c *counted) Write(p []byte) (int, error) {
	minute, err := c.b.reserveIO(uint64(len(p)), false)
	if err != nil {
		return 0, err
	}
	n, e := c.Conn.Write(p)
	c.b.refund(uint64(len(p)-n), false, uint64(n), minute)
	return n, e
}
