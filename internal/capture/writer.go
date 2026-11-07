package capture

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"syscall"
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

func StorageSafe(dir string, budget int64) error {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(dir, &stat); err != nil {
		return err
	}
	free := uint64(stat.Bavail) * uint64(stat.Bsize)
	total := stat.Blocks * uint64(stat.Bsize)
	minimum := total / 10
	if minimum < 1<<30 {
		minimum = 1 << 30
	}
	if free < minimum {
		return errors.New("storage_paused: low filesystem free space")
	}
	var bytes int64
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
			i, e := d.Info()
			if e != nil {
				return e
			}
			bytes += i.Size()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if bytes > budget*9/10 {
		return errors.New("storage_paused: data budget threshold")
	}
	return nil
}
