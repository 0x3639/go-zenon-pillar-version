package runner

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/netip"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/0x3639/go-zenon-pillar-version/internal/httpapi"
	"github.com/0x3639/go-zenon-pillar-version/internal/limits"
	"github.com/0x3639/go-zenon-pillar-version/internal/netguard"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/0x3639/go-zenon-pillar-version/internal/reference"
	"github.com/0x3639/go-zenon-pillar-version/internal/storage"
)

func Run(parent context.Context, cfg Config) error {
	if cfg.Mode == "feasibility" {
		return runFeasibility(parent, cfg)
	}
	return runService(parent, cfg)
}

type references struct {
	sync.Mutex
	header         protocol.Header
	observed       time.Time
	err            string
	reconciliation reference.Reconciliation
	pillarError    string
}

func runService(parent context.Context, cfg Config) (result error) {
	debug.SetMemoryLimit(320 << 20)
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	if cfg.Duration > 0 {
		var durationCancel context.CancelFunc
		ctx, durationCancel = context.WithTimeout(ctx, cfg.Duration)
		defer durationCancel()
	}
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
	key, err := identity(filepath.Join(cfg.DataDir, "identity.key"))
	if err != nil {
		return err
	}
	budget, err := limits.Open(filepath.Join(cfg.DataDir, "limits.json"), cfg.Ingress, cfg.Egress)
	if err != nil {
		return err
	}
	db, err := storage.Open(cfg.DataDir, cfg.DataBudget, cfg.Retention)
	if err != nil {
		return err
	}
	defer db.Close()
	client, err := reference.New(cfg.RPCURLs)
	if err != nil {
		return err
	}
	boot := randomID()
	writer := capture.NewBatchWriter(db.WriteBatch)
	engine := capture.NewEngine(cfg.ObserverID, boot, writer)
	engine.Rolling = true
	configBytes, _ := json.Marshal(cfg)
	fingerprint := fmt.Sprintf("%x", sha256.Sum256(configBytes))
	writer.Submit(map[string]any{"type": "run_started", "observerId": cfg.ObserverID, "bootId": boot, "release": Release, "upstreamRevision": UpstreamRevision, "configurationFingerprint": fingerprint, "startedAt": engine.Start.UTC()})
	status := &httpapi.Status{}
	status.Set(map[string]any{"captureState": "initializing", "release": Release, "bootId": boot}, false)
	api := httpapi.New(db, status, cfg.ObserverID, cfg.Retention, cfg.Origins)
	server, listener, err := httpapi.Listen(cfg.Port, api)
	if err != nil {
		writer.Close()
		return err
	}
	serverDone := make(chan error, 1)
	go func() {
		e := server.Serve(listener)
		if e != nil && !errors.Is(e, http.ErrServerClosed) {
			serverDone <- e
			cancel()
		} else {
			serverDone <- nil
		}
	}()
	defer func() {
		shutdown, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		if server.Shutdown(shutdown) != nil {
			server.Close()
		}
		if e := <-serverDone; result == nil && e != nil {
			result = e
		}
	}()
	slog.Info("observer started", "release", Release, "mode", "service", "port", cfg.Port, "runDuration", cfg.Duration.String(), "maxPeers", cfg.MaxPeers, "rawRetentionDays", cfg.Retention)

	refs := &references{}
	var intake atomic.Bool
	intake.Store(capture.StorageSafe(cfg.DataDir, cfg.DataBudget) == nil)
	var workers sync.WaitGroup
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(10 * time.Second)
		defer ticker.Stop()
		refresh := func() {
			if !intake.Load() {
				return
			}
			h, e := client.Frontier(ctx)
			refs.Lock()
			defer refs.Unlock()
			if e != nil {
				refs.err = e.Error()
				return
			}
			refs.header = h
			refs.observed = time.Now()
			refs.err = ""
			if intake.Load() {
				engine.Resolved(h)
				writer.Submit(map[string]any{"type": "header", "header": h, "at": refs.observed.UTC()})
			}
		}
		refresh()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				refresh()
			case hash := <-engine.Resolve:
				if !intake.Load() {
					engine.ResolveFailed(hash)
					continue
				}
				h, e := client.Resolve(ctx, hash)
				if e != nil {
					engine.ResolveFailed(hash)
				} else if intake.Load() {
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
	workers.Add(1)
	go func() {
		defer workers.Done()
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		anchorCtx, stop := context.WithTimeout(ctx, 2*time.Second)
		anchor, anchorErr := db.Anchor(anchorCtx)
		stop()
		reconciler := &reference.Reconciler{URLs: cfg.RPCURLs, Call: client.Call, Previous: anchor, ContinuityUnresolved: anchorErr != nil || db.ContinuityUnresolved(ctx)}
		lastPillars := time.Time{}
		// Wait briefly for the initial frontier, then revisit at most once a minute.
		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-timer.C:
			case <-ticker.C:
			}
			if !intake.Load() {
				continue
			}
			refs.Lock()
			height := refs.header.Height
			refs.Unlock()
			if height < 2 {
				continue
			}
			checkCtx, end := context.WithTimeout(ctx, 45*time.Second)
			result := reconciler.Check(checkCtx, height, writer.Submit)
			end()
			if !result.ContinuityEstablished && result.State == "continuity_unresolved" {
				writer.Submit(map[string]any{"type": "chain_continuity_lost", "bootId": boot, "at": time.Now().UTC()})
			}
			refs.Lock()
			refs.reconciliation = result
			refs.Unlock()
			if result.Reorganized || !result.ContinuityEstablished {
				writer.Submit(map[string]any{"type": "chain_discontinuity", "bootId": boot, "at": time.Now().UTC(), "state": result.State})
			}
			if time.Since(lastPillars) >= 5*time.Minute {
				lastPillars = time.Now()
				snapshotCtx, end := context.WithTimeout(ctx, 20*time.Second)
				e := reference.Pillars(snapshotCtx, cfg.RPCURLs[0], client.Call, writer.Submit)
				end()
				refs.Lock()
				refs.pillarError = ""
				if e != nil {
					refs.pillarError = e.Error()
				}
				refs.Unlock()
			}
		}
	}()

	type completed struct {
		id  string
		ip  netip.Addr
		err error
	}
	done := make(chan completed, 8)
	active := map[string]netip.Addr{}
	used := map[netip.Addr]bool{}
	dialGate := make(chan struct{}, 1)
	peersCtx, closePeers := context.WithCancel(ctx)
	defer func() { closePeers() }()
	peersPaused := false
	pausePeers := func() {
		intake.Store(false)
		if !peersPaused {
			closePeers()
			peersPaused = true
		}
	}
	seq, next := uint64(0), 0
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	state := "initializing"
	lastReport, lastLog, lastMaintenance, lastStorage := time.Time{}, time.Time{}, time.Time{}, time.Time{}
	storageErr := ""
	var storageStats capture.StorageStats
	maintenanceErr := ""
	var finalErr error
	maintenanceDone := make(chan error, 1)
	maintenanceBusy := false
	publish := func(final bool) {
		now := time.Now()
		engine.Tick(now)
		report := engine.Report(now)
		report["methodVersion"] = httpapi.MethodVersion
		report["captureState"] = state
		report["release"] = Release
		report["upstreamRevision"] = UpstreamRevision
		report["mode"] = "service"
		report["final"] = final
		report["statisticsWindowSeconds"] = 600
		report["windowStart"] = maxTime(engine.Start, now.Add(-10*time.Minute)).UTC()
		delete(report, "milestone1Passed")
		delete(report, "evidenceBytes")
		report["metadataBytesCommittedThisBoot"] = writer.Bytes.Load()
		report["traffic"] = budget.Snapshot()
		report["dataBudgetBytes"] = cfg.DataBudget
		report["storage"] = storageStats
		report["storageCheckError"] = storageErr
		report["maintenanceError"] = maintenanceErr
		report["rawRetentionDays"] = cfg.Retention
		report["aggregateRetentionImplemented"] = false
		report["schemaMigrationVersion"] = storage.SchemaVersion
		report["configurationFingerprint"] = fingerprint
		refs.Lock()
		report["referenceObservedAt"] = refs.observed
		report["referenceError"] = refs.err
		report["reconciliation"] = refs.reconciliation
		report["pillarSnapshotError"] = refs.pillarError
		refs.Unlock()
		healthy := 0
		for _, s := range report["sessions"].([]capture.Session) {
			if s.Ended == nil {
				healthy++
			}
		}
		report["connectedPeers"] = healthy
		report["configuredPeers"] = cfg.MaxPeers
		ready := !final && storageErr == "" && maintenanceErr == "" && !writer.Failed.Load() && budget.Error() == nil && !engine.Stopped() && writer.Drops.Load() == 0
		status.Set(report, ready)
		writer.Submit(map[string]any{"type": "run_status", "bootId": boot, "state": state, "at": now.UTC(), "drops": writer.Drops.Load(), "clockJumps": report["clockJumps"]})
		if final || now.Sub(lastLog) >= 30*time.Second {
			slog.Info("observer status", "state", state, "connectedPeers", healthy, "recentCheckedMomentums", report["distinctCheckedLiveMomentums"], "captureDrops", writer.Drops.Load(), "storageError", storageErr, "writerError", writer.Error())
			lastLog = now
		}
	}
	for ctx.Err() == nil {
		select {
		case <-ctx.Done():
		case e := <-maintenanceDone:
			maintenanceBusy = false
			maintenanceErr = ""
			if e != nil {
				maintenanceErr = e.Error()
			}
		case d := <-done:
			delete(active, d.id)
			delete(used, d.ip)
			reason := "completed"
			if d.err != nil {
				reason = d.err.Error()
			}
			cd := budget.Cooldown(d.id)
			cd.Failures++
			cd.Until = time.Now().Add(backoff(cd.Failures))
			if e := budget.SetCooldown(d.id, cd); e != nil {
				storageErr = e.Error()
			}
			writer.Submit(map[string]any{"type": "connection_attempt_ended", "bootId": boot, "peerId": d.id, "ip": d.ip.String(), "reason": reason, "at": time.Now().UTC()})
			slog.Info("peer session ended", "ip", d.ip, "reason", reason)
		case now := <-ticker.C:
			if now.Sub(lastStorage) >= 10*time.Second {
				storageErr = ""
				var e error
				storageStats, e = capture.MeasureStorage(cfg.DataDir)
				if e == nil {
					e = storageStats.Safe(cfg.DataBudget)
				}
				if e != nil {
					storageErr = e.Error()
				}
				lastStorage = now
			}
			maintenanceInterval := time.Minute
			if storageStats.UsedBytes >= cfg.DataBudget*8/10 {
				maintenanceInterval = 10 * time.Second
			}
			if !maintenanceBusy && now.Sub(lastMaintenance) >= maintenanceInterval {
				lastMaintenance = now
				maintenanceBusy = true
				workers.Add(1)
				go func() {
					defer workers.Done()
					mctx, end := context.WithTimeout(ctx, 5*time.Second)
					defer end()
					maintenanceDone <- db.Maintenance(mctx, time.Now())
				}()
			}
			switch {
			case storageErr != "" || maintenanceErr != "" || budget.Error() != nil || writer.Failed.Load():
				state = "storage_paused"
				pausePeers()
			case engine.Stopped() || writer.Drops.Load() > 0:
				state = "capture_paused"
				pausePeers()
			case budget.Paused():
				state = "traffic_paused"
				pausePeers()
			default:
				if peersPaused && len(active) == 0 {
					peersCtx, closePeers = context.WithCancel(ctx)
					peersPaused = false
				}
				intake.Store(!peersPaused)
				refs.Lock()
				fresh := time.Since(refs.observed) < 30*time.Second
				refs.Unlock()
				state = "observing"
				if !fresh {
					state = "reference_degraded"
				}
			}
			if now.Sub(lastReport) >= 10*time.Second {
				publish(false)
				lastReport = now
			}
			if peersPaused || state != "observing" || len(active) >= cfg.MaxPeers || len(dialGate) > 0 {
				continue
			}
			refs.Lock()
			frontier := refs.header
			refs.Unlock()
			for count := 0; count < len(cfg.Seeds); count++ {
				candidate := cfg.Seeds[next%len(cfg.Seeds)]
				next++
				id := candidate.ID.String()
				if _, ok := active[id]; ok || now.Before(budget.Cooldown(id).Until) {
					continue
				}
				// One candidate per tick, with a two-second DNS deadline. API requests run
				// independently and never wait for network discovery.
				dialGate <- struct{}{}
				active[id] = netip.Addr{}
				seq++
				sid := engine.SessionID(seq)
				workers.Add(1)
				// Reserve the actual IP before dialing; the loop owns membership maps.
				ips, e := resolveCandidate(peersCtx, candidate.Host)
				if e != nil {
					<-dialGate
					delete(active, id)
					workers.Done()
					cd := budget.Cooldown(id)
					cd.Failures++
					cd.Until = time.Now().Add(backoff(cd.Failures))
					_ = budget.SetCooldown(id, cd)
					writer.Submit(map[string]any{"type": "candidate_rejected", "bootId": boot, "peerId": id, "reason": e.Error(), "at": now.UTC()})
					break
				}
				ip := ips[0]
				if used[ip] {
					<-dialGate
					delete(active, id)
					workers.Done()
					break
				}
				active[id] = ip
				used[ip] = true
				sessionCtx := peersCtx
				var once sync.Once
				release := func() { once.Do(func() { <-dialGate }) }
				go func() {
					defer workers.Done()
					defer release()
					conn, e := netguard.DialIP(sessionCtx, ip, strconv.Itoa(int(candidate.Port)))
					if e == nil {
						e = session(sessionCtx, budget.Wrap(conn), key, candidate.ID, sid, frontier, engine, release)
					}
					done <- completed{id, ip, e}
				}()
				break
			}
		}
	}
	cancel()
	closePeers()
	workers.Wait()
	state = "stopped"
	publish(true)
	engine.FinalDuplicates()
	writer.Submit(map[string]any{"type": "run_stopped", "bootId": boot, "state": state, "stoppedAt": time.Now().UTC()})
	writer.Close()
	if writer.Failed.Load() {
		finalErr = errors.New(writer.Error())
	}
	checkpoint, end := context.WithTimeout(context.Background(), 3*time.Second)
	defer end()
	if e := db.Checkpoint(checkpoint); e != nil {
		finalErr = e
	}
	return finalErr
}
func maxTime(a, b time.Time) time.Time {
	if a.After(b) {
		return a
	}
	return b
}
func resolveCandidate(ctx context.Context, host string) ([]netip.Addr, error) {
	ctx, end := context.WithTimeout(ctx, 2*time.Second)
	defer end()
	return netguard.Resolve(ctx, host)
}
