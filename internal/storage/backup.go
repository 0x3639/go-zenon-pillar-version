package storage

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/mattn/go-sqlite3"
)

// Backup uses SQLite's online backup API, not a copy of a live database file.
// The output must be a new file under /data and counts against its budget.
func Backup(ctx context.Context, dir, name string, budget int64) (err error) {
	if filepath.Base(name) != name || !strings.HasSuffix(name, ".sqlite") || name == "observer.sqlite" {
		return errors.New("backup requires a new .sqlite filename without a path")
	}
	if err = capture.StorageSafe(dir, budget); err != nil {
		return err
	}
	source := filepath.Join(dir, "observer.sqlite")
	info, err := os.Stat(source)
	if err != nil {
		return err
	}
	var total int64
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, e error) error {
		if e != nil {
			return e
		}
		if !d.IsDir() {
			i, e := d.Info()
			if e != nil {
				return e
			}
			total += i.Size()
		}
		return nil
	})
	if err != nil {
		return err
	}
	if total+info.Size()+32<<20 > budget*9/10 {
		return errors.New("insufficient data budget for local backup")
	}
	target := filepath.Join(dir, name)
	f, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return err
	}
	f.Close()
	defer func() {
		if err != nil {
			os.Remove(target)
		}
	}()
	src, err := sql.Open("sqlite3", (&url.URL{Scheme: "file", Path: source}).String()+"?mode=ro&_busy_timeout=250")
	if err != nil {
		return err
	}
	defer src.Close()
	dst, err := sql.Open("sqlite3", (&url.URL{Scheme: "file", Path: target}).String()+"?_busy_timeout=250")
	if err != nil {
		return err
	}
	defer dst.Close()
	a, err := src.Conn(ctx)
	if err != nil {
		return err
	}
	defer a.Close()
	b, err := dst.Conn(ctx)
	if err != nil {
		return err
	}
	defer b.Close()
	err = a.Raw(func(rawSource any) error {
		return b.Raw(func(rawTarget any) error {
			job, e := rawTarget.(*sqlite3.SQLiteConn).Backup("main", rawSource.(*sqlite3.SQLiteConn), "main")
			if e != nil {
				return e
			}
			for {
				if ctx.Err() != nil {
					job.Finish()
					return ctx.Err()
				}
				if e = capture.StorageSafe(dir, budget); e != nil {
					job.Finish()
					return e
				}
				done, e := job.Step(128)
				if e != nil {
					job.Finish()
					return e
				}
				if done {
					return job.Finish()
				}
				select {
				case <-ctx.Done():
					job.Finish()
					return ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
		})
	})
	if err != nil {
		return err
	}
	var result string
	if err = b.QueryRowContext(ctx, "PRAGMA quick_check").Scan(&result); err != nil {
		return err
	}
	if result != "ok" {
		return errors.New("backup integrity check failed")
	}
	return nil
}
