package runner

import (
	"context"
	"crypto/ecdsa"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/big"
	"net/netip"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/0x3639/go-zenon-pillar-version/internal/limits"
	"github.com/0x3639/go-zenon-pillar-version/internal/netguard"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/0x3639/go-zenon-pillar-version/internal/reference"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/p2p/discover"
)

func identity(path string) (*ecdsa.PrivateKey, error) {
	info, e := os.Lstat(path)
	if e == nil {
		if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
			return nil, errors.New("identity must be a regular file with mode 0600")
		}
		return crypto.LoadECDSA(path)
	}
	if !os.IsNotExist(e) {
		return nil, e
	}
	key, e := crypto.GenerateKey()
	if e != nil {
		return nil, e
	}
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	_, e = f.WriteString(hex.EncodeToString(crypto.FromECDSA(key)))
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e != nil {
		return nil, e
	}
	if closeErr != nil {
		return nil, closeErr
	}
	return key, nil
}
func randomID() string {
	b := make([]byte, 16)
	if _, e := rand.Read(b); e != nil {
		panic(e)
	}
	return hex.EncodeToString(b)
}
func backoff(failures int) time.Duration {
	if failures < 1 {
		failures = 1
	}
	if failures > 8 {
		failures = 8
	}
	base := 10 * time.Second * time.Duration(1<<uint(failures-1))
	if base > 12*time.Minute {
		base = 12 * time.Minute
	}
	n, e := rand.Int(rand.Reader, big.NewInt(int64(base/4)))
	if e != nil {
		return base
	}
	return base + time.Duration(n.Int64())
}

func runFeasibility(parent context.Context, cfg Config) error {
	debug.SetMemoryLimit(320 << 20)
	ctx, cancel := context.WithTimeout(parent, cfg.Duration)
	defer cancel()
	if err := os.MkdirAll(cfg.DataDir, 0700); err != nil {
		return err
	}
	lock, err := os.OpenFile(filepath.Join(cfg.DataDir, "observer.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer lock.Close()
	if err = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return errors.New("another observer owns this data directory")
	}
	defer syscall.Flock(int(lock.Fd()), syscall.LOCK_UN)
	if err = capture.StorageSafe(cfg.DataDir, cfg.DataBudget); err != nil {
		return err
	}
	key, err := identity(filepath.Join(cfg.DataDir, "identity.key"))
	if err != nil {
		return err
	}
	budget, err := limits.Open(filepath.Join(cfg.DataDir, "limits.json"), cfg.Ingress, cfg.Egress)
	if err != nil {
		return err
	}
	boot := randomID()
	writer, err := capture.NewWriter(filepath.Join(cfg.DataDir, "evidence-"+boot+".jsonl"))
	if err != nil {
		return err
	}
	engine := capture.NewEngine(cfg.ObserverID, boot, writer)
	client, err := reference.New(cfg.RPCURLs)
	if err != nil {
		writer.Close()
		return err
	}
	fingerprintBytes, _ := json.Marshal(cfg)
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(fingerprintBytes))
	writer.Submit(map[string]any{"type": "run_started", "observerId": cfg.ObserverID, "bootId": boot, "release": Release, "upstreamRevision": UpstreamRevision, "configurationFingerprint": fingerprint, "startedAt": engine.Start.UTC(), "publicPeerId": discover.PubkeyID(&key.PublicKey).String()})
	var wg sync.WaitGroup
	var refMu sync.Mutex
	var frontier protocol.Header
	var referenceAt time.Time
	var referenceError string
	wg.Add(1)
	go func() {
		defer wg.Done()
		tick := time.NewTicker(10 * time.Second)
		defer tick.Stop()
		refresh := func() {
			h, e := client.Frontier(ctx)
			refMu.Lock()
			defer refMu.Unlock()
			if e != nil {
				referenceError = e.Error()
				return
			}
			frontier = h
			referenceAt = time.Now()
			referenceError = ""
			engine.Resolved(h)
		}
		refresh()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
				refresh()
			case hash := <-engine.Resolve:
				h, e := client.Resolve(ctx, hash)
				if e != nil {
					engine.ResolveFailed(hash)
				} else {
					engine.Resolved(h)
				}
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
			}
		}
	}()
	type done struct {
		id  string
		ip  netip.Addr
		err error
	}
	completed := make(chan done, 8)
	active := map[string]netip.Addr{}
	usedIP := map[netip.Addr]bool{}
	dialGate := make(chan struct{}, 1)
	seq := uint64(0)
	next := 0
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	state := "initializing"
	peakHeap := uint64(0)
	peakRSS := int64(0)
	lastReport := time.Time{}
	var finalErr error
	report := func(final bool) {
		now := time.Now()
		if !final {
			engine.Tick(now)
		}
		var mem runtime.MemStats
		runtime.ReadMemStats(&mem)
		if mem.HeapAlloc > peakHeap {
			peakHeap = mem.HeapAlloc
		}
		var usage syscall.Rusage
		_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
		if usage.Maxrss > peakRSS {
			peakRSS = usage.Maxrss
		}
		r := engine.Report(now)
		r["captureState"] = state
		r["final"] = final
		r["release"] = Release
		r["upstreamRevision"] = UpstreamRevision
		r["configurationFingerprint"] = fingerprint
		r["peakHeapBytes"] = peakHeap
		r["peakRSSBytes"] = peakRSS * 1024
		r["traffic"] = budget.Snapshot()
		if finalErr != nil || budget.Error() != nil {
			r["milestone1Passed"] = false
			r["captureState"] = "storage_paused"
			if finalErr != nil {
				r["failureReason"] = finalErr.Error()
			} else {
				r["failureReason"] = budget.Error().Error()
			}
		}
		refMu.Lock()
		r["referenceObservedAt"] = referenceAt
		r["referenceError"] = referenceError
		refMu.Unlock()
		if e := limits.AtomicJSON(filepath.Join(cfg.DataDir, "report-"+boot+".json"), r); e != nil {
			finalErr = e
			cancel()
		}
		if final {
			data, _ := json.Marshal(r)
			fmt.Println(string(data))
		}
	}
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case d := <-completed:
			delete(active, d.id)
			delete(usedIP, d.ip)
			cd := budget.Cooldown(d.id)
			cd.Failures++
			cd.Until = time.Now().Add(backoff(cd.Failures))
			if err = budget.SetCooldown(d.id, cd); err != nil {
				finalErr = err
				cancel()
			}
			reason := "completed"
			if d.err != nil {
				reason = d.err.Error()
			}
			writer.Submit(map[string]any{"type": "connection_attempt_ended", "peerId": d.id, "ip": d.ip.String(), "reason": reason, "retryAfter": cd.Until})
			slog.Info("peer session ended", "ip", d.ip, "reason", reason)
		case now := <-tick.C:
			if now.Sub(lastReport) >= 10*time.Second {
				report(false)
				lastReport = now
			}
			if e := budget.Error(); e != nil {
				state = "storage_paused"
				finalErr = e
				cancel()
				continue
			}
			if e := capture.StorageSafe(cfg.DataDir, cfg.DataBudget); e != nil {
				state = "storage_paused"
				finalErr = e
				cancel()
				continue
			}
			if engine.Stopped() {
				state = "capture_paused"
				cancel()
				continue
			}
			if budget.Paused() {
				state = "traffic_paused"
				cancel()
				continue
			}
			refMu.Lock()
			h := frontier
			fresh := time.Since(referenceAt) < 30*time.Second
			refMu.Unlock()
			if !fresh {
				state = "reference_degraded"
				continue
			}
			state = "observing"
			if len(active) >= cfg.MaxPeers || len(dialGate) > 0 {
				continue
			}
			for count := 0; count < len(cfg.Seeds); count++ {
				candidate := cfg.Seeds[next%len(cfg.Seeds)]
				next++
				id := candidate.ID.String()
				if _, ok := active[id]; ok {
					continue
				}
				if now.Before(budget.Cooldown(id).Until) {
					continue
				}
				dialCtx, stop := context.WithTimeout(ctx, 10*time.Second)
				ips, e := netguard.Resolve(dialCtx, candidate.Host)
				stop()
				if e != nil {
					writer.Submit(map[string]any{"type": "candidate_rejected", "peerId": id, "reason": e.Error()})
					cd := budget.Cooldown(id)
					cd.Failures++
					cd.Until = time.Now().Add(backoff(cd.Failures))
					if e = budget.SetCooldown(id, cd); e != nil {
						finalErr = e
						cancel()
					}
					continue
				}
				ip := ips[0]
				if usedIP[ip] {
					continue
				}
				active[id] = ip
				usedIP[ip] = true
				seq++
				sid := engine.SessionID(seq)
				dialGate <- struct{}{}
				var once sync.Once
				releaseDial := func() { once.Do(func() { <-dialGate }) }
				wg.Add(1)
				go func() {
					defer wg.Done()
					defer releaseDial()
					conn, e := netguard.DialIP(ctx, ip, strconv.Itoa(int(candidate.Port)))
					if e == nil {
						e = session(ctx, budget.Wrap(conn), key, candidate.ID, sid, h, engine, releaseDial)
					}
					completed <- done{id, ip, e}
				}()
				break
			}
		}
	}
	cancel()
	wg.Wait()
	engine.Tick(time.Now())
	engine.FinalDuplicates()
	writer.Submit(map[string]any{"type": "run_stopped", "bootId": boot, "stoppedAt": time.Now().UTC(), "state": state})
	writer.Close()
	if state == "observing" || state == "initializing" || state == "reference_degraded" {
		state = "stopped"
	}
	report(true)
	return finalErr
}
