package reference

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
)

const RecheckHeights = 180

type Caller func(context.Context, string, string, any, any) error
type Reconciliation struct {
	State                 string    `json:"state"`
	CheckedAt             time.Time `json:"checkedAt"`
	StartHeight           uint64    `json:"startHeight"`
	EndHeight             uint64    `json:"endHeight"`
	Sources               []string  `json:"sources"`
	DisputedHeights       int       `json:"disputedHeights"`
	Reorganized           bool      `json:"reorganized"`
	ContinuityEstablished bool      `json:"continuityEstablished"`
	Error                 string    `json:"error"`
}
type Reconciler struct {
	URLs                 []string
	Call                 Caller
	Previous             map[uint64]string
	ContinuityUnresolved bool
}

func (r *Reconciler) window(ctx context.Context, source string, start, end uint64) ([]protocol.Header, error) {
	all := make([]protocol.Header, 0, RecheckHeights)
	for base := start; base <= end; {
		count := min(uint64(30), end-base+1)
		var reply struct {
			List []*nom.Momentum `json:"list"`
		}
		if err := r.Call(ctx, source, "ledger.getMomentumsByHeight", []any{base, count}, &reply); err != nil {
			return nil, err
		}
		if len(reply.List) != int(count) {
			return nil, errors.New("incomplete reference window")
		}
		for i, m := range reply.List {
			h, e := protocol.CheckMomentum(m, source)
			if e != nil {
				return nil, e
			}
			if h.Height != base+uint64(i) {
				return nil, errors.New("unexpected reference height")
			}
			if len(all) > 0 && h.PreviousHash != all[len(all)-1].Hash {
				return nil, errors.New("reference window is not contiguous")
			}
			h.Verification = "rpc_observed"
			all = append(all, h)
		}
		base += count
	}
	return all, nil
}

func (r *Reconciler) Check(ctx context.Context, height uint64, submit func(any) bool) Reconciliation {
	out := Reconciliation{State: "reference_degraded", CheckedAt: time.Now().UTC(), Sources: []string{}, EndHeight: height}
	if height < 2 {
		out.Error = "no reference height"
		return out
	}
	start := uint64(2)
	if height >= RecheckHeights+1 {
		start = height - RecheckHeights + 1
	}
	out.StartHeight = start
	windows := [][]protocol.Header{}
	for _, u := range r.URLs {
		list, e := r.window(ctx, u, start, height)
		if e != nil {
			out.Error = e.Error()
			continue
		}
		windows = append(windows, list)
		out.Sources = append(out.Sources, u)
		for _, h := range list {
			if !submit(map[string]any{"type": "reference_checked", "header": h, "source": u, "at": out.CheckedAt}) {
				out.Error = "capture queue full"
				return out
			}
		}
		if len(windows) == 2 {
			break
		}
	}
	if len(windows) < 2 {
		out.Error = "two complete reference windows unavailable: " + out.Error
		return out
	}
	out.Error = ""
	out.State = "rpc_agreed"
	out.ContinuityEstablished = true
	current := map[uint64]string{}
	shared := false
	for i, a := range windows[0] {
		b := windows[1][i]
		disputed := a.Hash != b.Hash || a.Producer != b.Producer
		if disputed {
			out.DisputedHeights++
			out.State = "disputed"
		} else {
			current[a.Height] = a.Hash.String()
		}
		if old, ok := r.Previous[a.Height]; ok {
			if old == a.Hash.String() && !disputed {
				shared = true
			}
			if old != a.Hash.String() || disputed {
				out.Reorganized = true
			}
		}
		if !submit(map[string]any{"type": "chain_agreement", "height": a.Height, "agreedHash": a.Hash.String(), "disputed": disputed, "at": out.CheckedAt}) {
			out.State = "capture_degraded"
			out.Error = "capture queue full"
			return out
		}
	}
	if len(r.Previous) > 0 && !shared {
		r.ContinuityUnresolved = true
	}
	if r.ContinuityUnresolved {
		out.ContinuityEstablished = false
		out.State = "continuity_unresolved"
		out.Error = "no shared anchor within bounded recheck window; historical continuity requires operator review"
	}
	r.Previous = current
	return out
}

// Names are dated reference snapshots, never retroactive producer assignments.
func Pillars(ctx context.Context, source string, call Caller, submit func(any) bool) error {
	for page := 0; page < 4; page++ {
		var reply struct {
			Count int `json:"count"`
			List  []struct {
				Name     string `json:"name"`
				Producer string `json:"producerAddress"`
				Revoked  int64  `json:"revokeTimestamp"`
			} `json:"list"`
		}
		if e := call(ctx, source, "embedded.pillar.getAll", []any{page, 100}, &reply); e != nil {
			return e
		}
		if len(reply.List) > 100 || reply.Count > 400 {
			return errors.New("pillar snapshot size limit")
		}
		at := time.Now().UTC()
		for _, p := range reply.List {
			if len(p.Name) > 256 {
				return errors.New("pillar name too long")
			}
			if _, e := types.ParseAddress(p.Producer); e != nil {
				return fmt.Errorf("invalid registered producer: %w", e)
			}
			if p.Revoked != 0 {
				continue
			}
			if !submit(map[string]any{"type": "pillar_snapshot", "producerAddress": p.Producer, "name": p.Name, "source": source, "at": at}) {
				return errors.New("capture queue full")
			}
		}
		if len(reply.List) < 100 || (page+1)*100 >= reply.Count {
			return nil
		}
	}
	return nil
}
