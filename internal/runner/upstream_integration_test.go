//go:build upstream_integration

package runner

import (
	"context"
	"crypto/ed25519"
	"github.com/0x3639/go-zenon-pillar-version/internal/capture"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/p2p"
	"github.com/zenon-network/go-zenon/p2p/discover"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// A real upstream peer negotiates encryption, Hello and eth/61. It sends a
// signed full momentum and a hash without requiring a full chain or wallet.
func TestSessionAgainstPinnedUpstreamPeer(t *testing.T) {
	remoteKey, _ := crypto.GenerateKey()
	localKey, _ := crypto.GenerateKey()
	pub, priv, _ := ed25519.GenerateKey(nil)
	m := &nom.Momentum{Version: 1, ChainIdentifier: 1, Height: 22, PreviousHash: types.NewHash([]byte("prev")), TimestampUnix: uint64(time.Now().Unix()), PublicKey: pub}
	m.Hash = m.ComputeHash()
	m.Signature = ed25519.Sign(priv, m.Hash.Bytes())
	sent := make(chan struct{})
	stopRemote := make(chan struct{})
	var once sync.Once
	srv := &p2p.Server{PrivateKey: remoteKey, MaxPeers: 2, NoDial: true, Name: "v0.0.9 znn-node", ListenAddr: "127.0.0.1:0", Protocols: []p2p.Protocol{{Name: "eth", Version: 61, Length: 9, Run: func(peer *p2p.Peer, rw p2p.MsgReadWriter) error {
		if e := p2p.Send(rw, 0, protocol.Status{ProtocolVersion: 61, NetworkID: 1, Height: 22, CurrentBlock: m.Hash, GenesisBlock: protocol.Genesis}); e != nil {
			return e
		}
		msg, e := rw.ReadMsg()
		if e != nil {
			return e
		}
		if msg.Code != 0 {
			t.Error("missing observer status")
		}
		_ = msg.Discard()
		if e = p2p.Send(rw, 7, &nom.DetailedMomentum{Momentum: m}); e != nil {
			return e
		}
		if e = p2p.Send(rw, 1, []types.Hash{m.Hash}); e != nil {
			return e
		}
		once.Do(func() { close(sent) })
		<-stopRemote
		return nil
	}}}}
	if e := srv.Start(); e != nil {
		t.Fatal(e)
	}
	defer srv.Stop()
	defer close(stopRemote)
	writer, e := capture.NewWriter(filepath.Join(t.TempDir(), "events.jsonl"))
	if e != nil {
		t.Fatal(e)
	}
	engine := capture.NewEngine("test", "testboot", writer)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	conn, e := net.Dial("tcp", srv.ListenAddr)
	if e != nil {
		t.Fatal(e)
	}
	done := make(chan error, 1)
	go func() {
		done <- session(ctx, conn, localKey, discover.PubkeyID(&remoteKey.PublicKey), "testsession", protocol.Header{Hash: m.Hash, Height: 22}, engine, func() {})
	}()
	select {
	case <-sent:
	case <-ctx.Done():
		t.Fatal("upstream exchange timed out")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		r := engine.Report(time.Now())
		k := r["arrivalsByKind"].(map[string]int)
		if k["full_broadcast"] == 1 && k["hash_announcement"] == 1 {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done
	writer.Close()
	r := engine.Report(time.Now())
	k := r["arrivalsByKind"].(map[string]int)
	if k["full_broadcast"] != 1 || k["hash_announcement"] != 1 {
		t.Fatalf("events: %v", k)
	}
}
