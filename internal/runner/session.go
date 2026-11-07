package runner

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"net"
	"regexp"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/0x3639/go-zenon-pillar-version/internal/transport"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/p2p"
	"github.com/zenon-network/go-zenon/p2p/discover"
)

var versionRE = regexp.MustCompile(`(?:^|[/\s])v?(\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?)(?:$|[/\s])`)

func version(name string) *string {
	m := versionRE.FindStringSubmatch(name)
	if len(m) < 2 {
		return nil
	}
	v := m[1]
	return &v
}

func session(ctx context.Context, conn net.Conn, key *ecdsa.PrivateKey, id discover.NodeID, sid string, frontier protocol.Header, engine *capture.Engine, releaseDial func()) (result error) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	defer stop()
	defer releaseDial()
	started := time.Now()
	c, hello, err := transport.DialHandshake(ctx, conn, key, id, "zenon-observer/"+Release)
	if err != nil {
		return fmt.Errorf("transport/Hello: %w", err)
	}
	timer := time.AfterFunc(8*time.Second, func() { conn.Close() })
	if err = protocol.SendStatus(c, frontier); err != nil {
		timer.Stop()
		return err
	}
	statusOK := false
	for attempts := 0; attempts < 8; attempts++ {
		msg, err := c.ReadMsg()
		if err != nil {
			timer.Stop()
			return fmt.Errorf("status: %w", err)
		}
		if msg.Code == 2 {
			if msg.Size > 2048 {
				timer.Stop()
				return errors.New("oversized ping")
			}
			if err = p2p.SendItems(c, 3); err != nil {
				timer.Stop()
				return err
			}
			continue
		}
		if msg.Code == 1 {
			timer.Stop()
			return disconnectReason(msg)
		}
		if msg.Code != protocol.Offset+protocol.StatusMsg {
			timer.Stop()
			return errors.New("expected status after Hello")
		}
		b, err := protocol.Payload(msg)
		if err != nil {
			timer.Stop()
			return err
		}
		var s protocol.Status
		if err = protocol.Decode(b, &s); err != nil {
			timer.Stop()
			return err
		}
		if err = protocol.CheckStatus(s); err != nil {
			timer.Stop()
			return err
		}
		statusOK = true
		break
	}
	if !timer.Stop() || !statusOK {
		return errors.New("status handshake deadline")
	}
	ready := time.Now()
	addr := conn.RemoteAddr().(*net.TCPAddr)
	caps := []string{}
	for _, cap := range hello.Capabilities {
		caps = append(caps, cap.String())
	}
	engine.AddSession(capture.Session{ID: sid, PeerID: id.String(), IP: addr.IP.String(), Port: uint16(addr.Port), ClientName: hello.Name, Version: version(hello.Name), Capabilities: caps, Started: started.UTC(), Ready: ready, Reference: frontier})
	defer func() {
		reason := "run_complete"
		if result != nil {
			reason = result.Error()
		}
		engine.EndSession(sid, reason)
	}()
	releaseDial()
	pingCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	pingDone := make(chan struct{})
	go func() {
		defer close(pingDone)
		tick := time.NewTicker(15 * time.Second)
		defer tick.Stop()
		for {
			select {
			case <-pingCtx.Done():
				return
			case <-tick.C:
				if err := p2p.SendItems(c, 2); err != nil {
					conn.Close()
					return
				}
			}
		}
	}()
	defer func() { cancel(); conn.Close(); <-pingDone }()
	pending := map[types.Hash]time.Time{}
	lastRequest := time.Time{}
	for {
		msg, err := c.ReadMsg()
		if err != nil {
			return err
		}
		at := msg.ReceivedAt // captured inside the transport before frame decoding
		if msg.Code < protocol.Offset {
			if msg.Size > 2048 {
				return errors.New("base message size limit")
			}
			switch msg.Code {
			case 1:
				return disconnectReason(msg)
			case 2:
				if err = p2p.SendItems(c, 3); err != nil {
					return err
				}
			case 3:
			default:
				return errors.New("unexpected base protocol message")
			}
			continue
		}
		code := msg.Code - protocol.Offset
		if code == protocol.TxMsg {
			if msg.Size > protocol.MaxMessageSize {
				return errors.New("transaction byte limit")
			}
			if err = msg.Discard(); err != nil {
				return err
			}
			continue
		}
		b, err := protocol.Payload(msg)
		if err != nil {
			return err
		}
		switch code {
		case protocol.NewBlockHashesMsg:
			hashes, err := protocol.Hashes(b)
			if err != nil {
				return err
			}
			for _, hash := range hashes {
				if hash.IsZero() {
					return errors.New("zero announced hash")
				}
				engine.Observe(sid, hash, "hash_announcement", at, nil)
			}
			for hash, deadline := range pending {
				if at.After(deadline) {
					delete(pending, hash)
				}
			}
			// At most one recent announced hash request per session per 10s.
			if len(hashes) > 0 && len(pending) == 0 && at.Sub(lastRequest) >= 10*time.Second && at.Sub(ready) >= 5*time.Second {
				hash := hashes[len(hashes)-1]
				pending[hash] = at.Add(30 * time.Second)
				lastRequest = at
				if err = p2p.Send(c, protocol.Offset+protocol.GetBlocksMsg, []types.Hash{hash}); err != nil {
					return err
				}
			}
		case protocol.NewBlockMsg:
			h, err := protocol.Full(b)
			if err != nil {
				return err
			}
			engine.Observe(sid, h.Hash, "full_broadcast", at, &h)
			engine.Resolved(h)
		case protocol.BlocksMsg:
			var blocks []rlp.RawValue
			if err = protocol.Decode(b, &blocks); err != nil {
				return err
			}
			if len(blocks) > 16 {
				return errors.New("block response count limit")
			}
			for _, raw := range blocks {
				h, err := protocol.Full(raw)
				if err != nil {
					return err
				}
				kind := "historical_or_unresolved"
				if deadline, ok := pending[h.Hash]; ok && !at.After(deadline) {
					kind = "requested_response"
					delete(pending, h.Hash)
				}
				engine.Observe(sid, h.Hash, kind, at, &h)
				engine.Resolved(h)
			}
		case protocol.GetBlocksMsg, protocol.GetBlockHashesMsg, protocol.GetBlockHashesFromNumberMsg:
			if err = protocol.ValidateRequest(code, b); err != nil {
				return err
			}
			// This experiment caches headers, not detailed transaction bodies.
			if code == protocol.GetBlocksMsg {
				err = p2p.Send(c, protocol.Offset+protocol.BlocksMsg, []*nom.DetailedMomentum{})
			} else {
				err = p2p.Send(c, protocol.Offset+protocol.BlockHashesMsg, []types.Hash{})
			}
			if err != nil {
				return err
			}
		case protocol.BlockHashesMsg:
			if _, err = protocol.Hashes(b); err != nil {
				return err
			}
		case protocol.StatusMsg:
			return errors.New("unexpected duplicate status")
		default:
			return fmt.Errorf("unsupported message code %d", code)
		}
	}
}
func disconnectReason(msg p2p.Msg) error {
	b, e := protocol.Payload(msg)
	if e != nil {
		return e
	}
	var reason [1]p2p.DiscReason
	if e = protocol.Decode(b, &reason); e != nil {
		return e
	}
	return fmt.Errorf("remote disconnect: %s", reason[0])
}
