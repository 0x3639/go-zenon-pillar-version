// SPDX-License-Identifier: LGPL-3.0-or-later
// Adapter for the pinned Zenon RLPx implementation. No upstream Server or
// ProtocolManager is started. See third_party/NOTICE.md for modifications.
package transport

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"net"
	"time"

	"github.com/zenon-network/go-zenon/p2p"
	"github.com/zenon-network/go-zenon/p2p/discover"
)

const (
	MaxMessageSize         = 10 * 1024 * 1024
	baseProtocolVersion    = 4
	baseProtocolMaxMsgSize = 2048
	frameReadTimeout       = 45 * time.Second
	frameWriteTimeout      = 5 * time.Second
	handshakeMsg           = 0
	discMsg                = 1
)

type Msg = p2p.Msg
type MsgReader = p2p.MsgReader
type DiscReason = p2p.DiscReason
type Cap = p2p.Cap

const (
	DiscNetworkError        = p2p.DiscNetworkError
	DiscIncompatibleVersion = p2p.DiscIncompatibleVersion
	DiscInvalidIdentity     = p2p.DiscInvalidIdentity
)

var Send = p2p.Send
var SendItems = p2p.SendItems

type protoHandshake struct {
	Version    uint64
	Name       string
	Caps       []Cap
	ListenPort uint64
	ID         discover.NodeID
}

type Hello struct {
	PeerID       string `json:"peerId"`
	Name         string `json:"clientName"`
	Capabilities []Cap  `json:"capabilities"`
}

type Connection struct{ *rlpx }

func DialHandshake(ctx context.Context, conn net.Conn, key *ecdsa.PrivateKey, expected discover.NodeID, name string) (*Connection, Hello, error) {
	t := newRLPX(conn)
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	remote, err := t.doEncHandshake(key, &discover.Node{ID: expected})
	if err != nil {
		return nil, Hello{}, err
	}
	if remote != expected {
		return nil, Hello{}, errors.New("encrypted peer identity mismatch")
	}
	hs, err := t.doProtoHandshake(&protoHandshake{Version: 4, Name: name, Caps: []Cap{{Name: "eth", Version: 61}}, ID: discover.PubkeyID(&key.PublicKey)})
	if err != nil {
		return nil, Hello{}, err
	}
	if hs.ID != expected {
		return nil, Hello{}, errors.New("Hello peer identity mismatch")
	}
	found := false
	for _, cap := range hs.Caps {
		if cap.Name == "eth" && cap.Version == 61 {
			found = true
		}
	}
	if !found {
		return nil, Hello{}, errors.New("eth/61 capability missing")
	}
	_ = conn.SetDeadline(time.Time{})
	return &Connection{t}, Hello{PeerID: expected.String(), Name: hs.Name, Capabilities: hs.Caps}, nil
}

func (c *Connection) Disconnect(reason p2p.DiscReason) { c.close(reason) }
