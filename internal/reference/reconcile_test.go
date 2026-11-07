package reference

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
)

func chain(t *testing.T, n int) []*nom.Momentum {
	t.Helper()
	pub, key, e := ed25519.GenerateKey(nil)
	if e != nil {
		t.Fatal(e)
	}
	prev := types.NewHash(pub)
	list := []*nom.Momentum{}
	for i := 0; i < n; i++ {
		m := &nom.Momentum{Version: 1, ChainIdentifier: 1, Height: uint64(i + 2), PreviousHash: prev, TimestampUnix: uint64(time.Now().Unix() - int64(n-i)*10), PublicKey: pub}
		m.Hash = m.ComputeHash()
		m.Signature = ed25519.Sign(key, m.Hash.Bytes())
		list = append(list, m)
		prev = m.Hash
	}
	return list
}
func TestReconciliationAgreementDisagreementAndDeepGap(t *testing.T) {
	first, second := chain(t, 185), chain(t, 185)
	divergent := false
	unavailable := false
	r := &Reconciler{URLs: []string{"one", "two"}}
	r.Call = func(_ context.Context, source, method string, params any, out any) error {
		if unavailable && source == "two" {
			return errors.New("RPC unavailable")
		}
		p := params.([]any)
		start, count := p[0].(uint64), p[1].(uint64)
		if count > 30 {
			t.Fatal("oversized request")
		}
		c := first
		if divergent && source == "two" {
			c = second
		}
		b, _ := json.Marshal(map[string]any{"list": c[start-2 : start-2+count]})
		return json.Unmarshal(b, out)
	}
	events := []any{}
	submit := func(v any) bool { events = append(events, v); return true }
	result := r.Check(context.Background(), 181, submit)
	if result.State != "rpc_agreed" || len(r.Previous) != 180 || !result.ContinuityEstablished {
		t.Fatal(result)
	}
	divergent = true
	result = r.Check(context.Background(), 182, submit)
	if result.DisputedHeights != 180 || result.State != "continuity_unresolved" {
		t.Fatal(result)
	}
	unavailable = true
	result = r.Check(context.Background(), 182, submit)
	if result.State != "reference_degraded" {
		t.Fatal(result)
	}
	// Losing a bounded shared anchor does not silently trigger historical sync.
	divergent = false
	unavailable = false
	r.Previous = map[uint64]string{1: "old"}
	result = r.Check(context.Background(), 182, submit)
	if result.ContinuityEstablished {
		t.Fatal("invented historical continuity")
	}
}
