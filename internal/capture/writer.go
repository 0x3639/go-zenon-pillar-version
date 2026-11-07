package capture

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"time"
)

const QueueSize = 2000
const MaxRecordBytes = 8192
const EvidenceLimit = 64 * 1024 * 1024

type Writer struct {
	queue  chan []byte
	done   chan struct{}
	Drops  atomic.Uint64
	Bytes  atomic.Uint64
	Failed atomic.Bool
	err    atomic.Pointer[string]
}

// NewBatchWriter keeps database work off socket readers. A failed transaction
// fails closed; every uncommitted record is counted as dropped.
func NewBatchWriter(write func([][]byte) error) *Writer {
	w := &Writer{queue: make(chan []byte, QueueSize), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		tick := time.NewTicker(250 * time.Millisecond)
		defer tick.Stop()
		batch := make([][]byte, 0, 100)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			if !w.Failed.Load() {
				if err := write(batch); err != nil {
					w.fail(err)
				}
			}
			if w.Failed.Load() {
				w.Drops.Add(uint64(len(batch)))
			} else {
				for _, b := range batch {
					w.Bytes.Add(uint64(len(b)))
				}
			}
			clear(batch)
			batch = batch[:0]
		}
		for {
			select {
			case b, ok := <-w.queue:
				if !ok {
					flush()
					return
				}
				batch = append(batch, b)
				if len(batch) == 100 {
					flush()
				}
			case <-tick.C:
				flush()
			}
		}
	}()
	return w
}

func NewWriter(path string) (*Writer, error) {
	f, e := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return nil, e
	}
	w := &Writer{queue: make(chan []byte, QueueSize), done: make(chan struct{})}
	go func() {
		defer close(w.done)
		defer f.Close()
		for b := range w.queue {
			if w.Failed.Load() {
				w.Drops.Add(1)
				continue
			}
			if w.Bytes.Load()+uint64(len(b)) > EvidenceLimit {
				w.fail(errors.New("evidence file limit reached"))
				w.Drops.Add(1)
				continue
			}
			n, err := f.Write(b)
			w.Bytes.Add(uint64(n))
			if err != nil {
				w.fail(err)
				w.Drops.Add(1)
			}
		}
		if err := f.Sync(); err != nil {
			w.fail(err)
		}
	}()
	return w, nil
}
func (w *Writer) fail(e error) { s := e.Error(); w.err.Store(&s); w.Failed.Store(true) }
func (w *Writer) Submit(v any) bool {
	b, e := json.Marshal(v)
	if e != nil || len(b) > MaxRecordBytes || w.Failed.Load() {
		w.Drops.Add(1)
		return false
	}
	b = append(b, '\n')
	select {
	case w.queue <- b:
		return true
	default:
		w.Drops.Add(1)
		return false
	}
}
func (w *Writer) Close()          { close(w.queue); <-w.done }
func (w *Writer) QueueDepth() int { return len(w.queue) }
func (w *Writer) Error() string {
	p := w.err.Load()
	if p == nil {
		return ""
	}
	return *p
}

type StorageStats struct {
	UsedBytes   int64  `json:"usedBytes"`
	FreeBytes   uint64 `json:"filesystemFreeBytes"`
	TotalBytes  uint64 `json:"filesystemTotalBytes"`
	MinimumFree uint64 `json:"minimumFreeBytes"`
}

func (s StorageStats) Safe(budget int64) error {
	if s.FreeBytes < s.MinimumFree {
		return errors.New("storage_paused: low filesystem free space")
	}
	if s.UsedBytes > budget*9/10 {
		return errors.New("storage_paused: data budget threshold")
	}
	return nil
}
func StorageSafe(dir string, budget int64) error {
	s, e := MeasureStorage(dir)
	if e != nil {
		return e
	}
	return s.Safe(budget)
}
func MeasureStorage(dir string) (StorageStats, error) {
	var s StorageStats
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return s, err
	}
	// f_blocks/f_bavail use the fragment size, not the preferred I/O size.
	unit := uint64(stat.Frsize)
	if unit == 0 {
		unit = uint64(stat.Bsize)
	}
	s.FreeBytes = uint64(stat.Bavail) * unit
	s.TotalBytes = stat.Blocks * unit
	s.MinimumFree = max(s.TotalBytes/10, 1<<30)
	entries := 0
	err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		entries++
		if entries > 10000 {
			return errors.New("storage_paused: file count limit")
		}
		if d.Type()&os.ModeSymlink != 0 {
			return errors.New("symlinks forbidden in observer data")
		}
		if !d.IsDir() {
			info, e := d.Info()
			if e != nil {
				return e
			}
			s.UsedBytes += info.Size()
		}
		return nil
	})
	return s, err
}
