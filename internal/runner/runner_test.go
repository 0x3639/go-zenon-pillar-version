package runner

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDedicatedIdentityPersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "identity.key")
	a, e := identity(path)
	if e != nil {
		t.Fatal(e)
	}
	b, e := identity(path)
	if e != nil {
		t.Fatal(e)
	}
	if a.D.Cmp(b.D) != 0 {
		t.Fatal("rotated identity")
	}
	if e = os.Chmod(path, 0644); e != nil {
		t.Fatal(e)
	}
	if _, e = identity(path); e == nil {
		t.Fatal("accepted permissive key file")
	}
}
func TestVersionUnknownAndBounds(t *testing.T) {
	if version("mystery node") != nil {
		t.Fatal("invented version")
	}
	if v := version("v0.0.9 znn-node"); v == nil || *v != "0.0.9" {
		t.Fatal("version parse")
	}
	for i := 1; i < 30; i++ {
		d := backoff(i)
		if d < 10*time.Second || d > 15*time.Minute {
			t.Fatal(d)
		}
	}
}
