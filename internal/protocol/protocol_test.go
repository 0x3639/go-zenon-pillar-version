package protocol

import (
	"bytes"
	"crypto/ed25519"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
)

func signed(t *testing.T) *nom.Momentum {
	t.Helper()
	pub, priv, e := ed25519.GenerateKey(nil)
	if e != nil {
		t.Fatal(e)
	}
	m := &nom.Momentum{Version: 1, ChainIdentifier: 1, Height: 42, PreviousHash: types.NewHash([]byte("previous")), TimestampUnix: uint64(time.Now().Unix()), PublicKey: pub}
	m.Hash = m.ComputeHash()
	m.Signature = ed25519.Sign(priv, m.Hash.Bytes())
	return m
}
func TestPinnedSignatureAndHash(t *testing.T) {
	m := signed(t)
	h, e := CheckMomentum(m, "test")
	if e != nil {
		t.Fatal(e)
	}
	if h.Producer != types.PubKeyToAddress(m.PublicKey).String() {
		t.Fatal("wrong producer")
	}
	m.Signature[0] ^= 1
	if _, e = CheckMomentum(m, "test"); e == nil {
		t.Fatal("accepted invalid signature")
	}
	m.Signature[0] ^= 1
	m.Height++
	if _, e = CheckMomentum(m, "test"); e == nil {
		t.Fatal("accepted changed hash")
	}
}
func TestBoundedDecoding(t *testing.T) {
	b, _ := rlp.EncodeToBytes(&nom.DetailedMomentum{Momentum: signed(t)})
	if _, e := Full(b); e != nil {
		t.Fatal(e)
	}
	if _, e := Full(append(b, 0x80)); e == nil {
		t.Fatal("accepted trailing bytes")
	}
	b, _ = rlp.EncodeToBytes(make([]types.Hash, MaxHashes+1))
	if _, e := Hashes(b); e == nil {
		t.Fatal("accepted oversized hash list")
	}
	b, _ = rlp.EncodeToBytes(make([]uint64, 5000))
	if e := Preflight(b); e == nil {
		t.Fatal("accepted expanded list")
	}
	nested := rlp.RawValue{0xc0}
	for i := 0; i < 20; i++ {
		nested, _ = rlp.EncodeToBytes([]rlp.RawValue{nested})
	}
	if e := Preflight(nested); e == nil {
		t.Fatal("accepted deep RLP")
	}
	if e := Preflight(bytes.Repeat([]byte{0x80}, MaxMessageSize+1)); e == nil {
		t.Fatal("accepted oversized payload")
	}
}
func TestStatusRejectsWrongNetwork(t *testing.T) {
	s := Status{61, 1, 42, types.NewHash([]byte("head")), Genesis}
	if e := CheckStatus(s); e != nil {
		t.Fatal(e)
	}
	s.NetworkID = 2
	if CheckStatus(s) == nil {
		t.Fatal("accepted other network")
	}
	s.NetworkID = 1
	s.GenesisBlock = types.ZeroHash
	if CheckStatus(s) == nil {
		t.Fatal("accepted wrong genesis")
	}
	s.GenesisBlock = Genesis
	s.ProtocolVersion = 62
	if CheckStatus(s) == nil {
		t.Fatal("accepted wrong version")
	}
}
func TestWarmupAndLiveWindow(t *testing.T) {
	now := time.Now()
	h := Header{Timestamp: uint64(now.Unix())}
	if Exclusion(h, now, now.Add(-4*time.Second)) != "warmup" {
		t.Fatal("warmup")
	}
	if Exclusion(h, now, now.Add(-6*time.Second)) != "" {
		t.Fatal("live")
	}
	h.Timestamp -= 121
	if Exclusion(h, now, now.Add(-6*time.Second)) != "outside_live_window" {
		t.Fatal("old")
	}
}
func FuzzFull(f *testing.F) {
	f.Add([]byte{0xc0})
	f.Fuzz(func(t *testing.T, b []byte) {
		if len(b) > 65536 {
			t.Skip()
		}
		_, _ = Full(b)
	})
}
