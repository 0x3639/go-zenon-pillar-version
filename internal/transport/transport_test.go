package transport

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/crypto"
	"github.com/zenon-network/go-zenon/p2p/discover"
	"golang.org/x/crypto/sha3"
)

func TestPinnedEncryptedHello(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	local, _ := crypto.GenerateKey()
	remote, _ := crypto.GenerateKey()
	done := make(chan error, 1)
	go func() {
		r := newRLPX(b)
		id, e := r.doEncHandshake(remote, nil)
		if e != nil {
			done <- e
			return
		}
		if id != discover.PubkeyID(&local.PublicKey) {
			done <- errors.New("wrong initiator identity")
			return
		}
		_, e = r.doProtoHandshake(&protoHandshake{Version: 4, Name: "v0.0.9 znn-node", Caps: []Cap{{Name: "eth", Version: 61}}, ID: discover.PubkeyID(&remote.PublicKey)})
		if e == nil {
			m, err := r.ReadMsg()
			e = err
			if e == nil && m.Code != 2 {
				e = errors.New("ping framing")
			}
		}
		done <- e
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	c, h, e := DialHandshake(ctx, a, local, discover.PubkeyID(&remote.PublicKey), "zenon-observer/test")
	if e != nil {
		t.Fatal(e)
	}
	if h.Name != "v0.0.9 znn-node" {
		t.Fatal("wrong Hello")
	}
	if e = SendItems(c, 2); e != nil {
		t.Fatal(e)
	}
	if e = <-done; e != nil {
		t.Fatal(e)
	}
}
func TestOversizedFrameRejectedBeforePayloadRead(t *testing.T) {
	key := make([]byte, 16)
	block, _ := aes.NewCipher(key)
	enc := cipher.NewCTR(block, make([]byte, 16))
	mac := sha3.New256()
	header := make([]byte, 32)
	putInt24(MaxMessageSize+2, header)
	copy(header[3:], zeroHeader)
	enc.XORKeyStream(header[:16], header[:16])
	copy(header[16:], updateMAC(mac, block, header[:16]))
	rw := newRLPXFrameRW(bytes.NewBuffer(header), secrets{AES: key, MAC: key, EgressMAC: sha3.New256(), IngressMAC: sha3.New256()})
	_, e := rw.ReadMsg()
	if e == nil || !strings.Contains(e.Error(), "exceeds observer limit") {
		t.Fatalf("payload was read before rejection: %v", e)
	}
}
func TestHelloIdentityMismatch(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	local, _ := crypto.GenerateKey()
	remote, _ := crypto.GenerateKey()
	other, _ := crypto.GenerateKey()
	done := make(chan error, 1)
	go func() {
		r := newRLPX(b)
		_, e := r.doEncHandshake(remote, nil)
		if e == nil {
			_, e = r.doProtoHandshake(&protoHandshake{Version: 4, Name: "test", Caps: []Cap{{Name: "eth", Version: 61}}, ID: discover.PubkeyID(&other.PublicKey)})
		}
		done <- e
	}()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _, e := DialHandshake(ctx, a, local, discover.PubkeyID(&remote.PublicKey), "test")
	if e == nil || !strings.Contains(e.Error(), "identity mismatch") {
		t.Fatalf("accepted mismatched Hello identity: %v", e)
	}
	<-done
}
