package runner

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestServiceAPIWithRPCOutageAndStoragePressure(t *testing.T) {
	for _, pressure := range []bool{false, true} {
		t.Run(strconv.FormatBool(pressure), func(t *testing.T) {
			dir := t.TempDir()
			budget := int64(2 << 30)
			if pressure {
				budget = 1 << 20
				if e := os.WriteFile(filepath.Join(dir, "existing-evidence"), make([]byte, 950*1024), 0600); e != nil {
					t.Fatal(e)
				}
			}
			l, e := net.Listen("tcp", "127.0.0.1:0")
			if e != nil {
				t.Fatal(e)
			}
			port := l.Addr().(*net.TCPAddr).Port
			l.Close()
			cfg := Config{ObserverID: "service-test", Mode: "service", DataDir: dir, MaxPeers: 2, RPCURLs: []string{"wss://8.8.8.8:1"}, Ingress: 250 << 20, Egress: 50 << 20, DataBudget: budget, Port: port, Retention: 7}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- Run(ctx, cfg) }()
			base := "http://127.0.0.1:" + strconv.Itoa(port)
			client := http.Client{Timeout: time.Second}
			deadline := time.Now().Add(5 * time.Second)
			for {
				r, e := client.Get(base + "/api/v1/status")
				if e == nil {
					var v struct {
						Result map[string]any `json:"result"`
					}
					json.NewDecoder(r.Body).Decode(&v)
					r.Body.Close()
					if v.Result["captureState"] != nil && v.Result["captureState"] != "initializing" {
						break
					}
				}
				if time.Now().After(deadline) {
					t.Fatal("API never became available")
				}
				time.Sleep(50 * time.Millisecond)
			}
			expected := 200
			if pressure {
				expected = 503
			}
			r, e := client.Get(base + "/readyz")
			if e != nil {
				t.Fatal(e)
			}
			r.Body.Close()
			if r.StatusCode != expected {
				t.Fatalf("readiness %d expected %d", r.StatusCode, expected)
			}
			r, e = client.Get(base + "/healthz")
			if e != nil {
				t.Fatal(e)
			}
			r.Body.Close()
			if r.StatusCode != 200 {
				t.Fatal("liveness depends on RPC/storage")
			}
			cancel()
			select {
			case e := <-done:
				if e != nil {
					t.Fatal(e)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("shutdown hung")
			}
		})
	}
}
func TestServiceConfigAndExplicitFeasibility(t *testing.T) {
	t.Setenv("PEER_SEEDS", "enode://0316e6152bed5773850c72a79a8eed9c4dd72966d6f5101e860fa73da6c9a064701964cf3e0a09420a60c38000497af4c43c0e1374bfe5120dee0180f7bc30ed@51.81.209.53:35995")
	t.Setenv("MODE", "service")
	t.Setenv("RUN_DURATION", "0s")
	t.Setenv("ALLOWED_ORIGINS", "https://0x3639.com")
	c, e := Load()
	if e != nil || c.Duration != 0 || c.Port != 8080 {
		t.Fatal(c, e)
	}
	t.Setenv("MODE", "feasibility")
	if _, e = Load(); e == nil {
		t.Fatal("unbounded feasibility")
	}
	t.Setenv("RUN_DURATION", "65m")
	if _, e = Load(); e != nil {
		t.Fatal(e)
	}
	t.Setenv("ALLOWED_ORIGINS", "https://0x3639.com/go-zenon-version/")
	if _, e = Load(); e == nil {
		t.Fatal("accepted path as CORS origin")
	}
}
