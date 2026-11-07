// SPDX-License-Identifier: GPL-3.0-or-later
// Wire layouts match go-zenon protocol/protocol.go at the pinned revision.
package protocol

import (
	"crypto/ed25519"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ethereum/go-ethereum/rlp"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
	"github.com/zenon-network/go-zenon/p2p"
)

const (
	Offset    = 16
	StatusMsg = iota - 1
	NewBlockHashesMsg
	TxMsg
	GetBlockHashesMsg
	BlockHashesMsg
	GetBlocksMsg
	BlocksMsg
	NewBlockMsg
	GetBlockHashesFromNumberMsg
)
const MaxHashes = 256
const MaxMessageSize = 10 * 1024 * 1024

var Genesis = types.HexToHashPanic("9e204601d1b7b1427fe12bc82622e610d8a6ad43c40abf020eb66e538bb8eeb0")

type Status struct {
	ProtocolVersion uint32
	NetworkID       uint32
	Height          uint64 // Upstream calls this TD; it is momentum height.
	CurrentBlock    types.Hash
	GenesisBlock    types.Hash
}

func CheckStatus(s Status) error {
	if s.ProtocolVersion != 61 || s.NetworkID != 1 || s.GenesisBlock != Genesis {
		return errors.New("Zenon mainnet status mismatch")
	}
	if s.Height == 0 || s.CurrentBlock.IsZero() {
		return errors.New("invalid status frontier")
	}
	return nil
}
func SendStatus(rw p2p.MsgWriter, header Header) error {
	if header.Height < 2 || header.Hash.IsZero() {
		return errors.New("real reference frontier required")
	}
	return p2p.Send(rw, Offset+StatusMsg, Status{61, 1, header.Height, header.Hash, Genesis})
}

type Header struct {
	Hash         types.Hash `json:"hash"`
	Height       uint64     `json:"height"`
	PreviousHash types.Hash `json:"previousHash"`
	Timestamp    uint64     `json:"timestamp"`
	Producer     string     `json:"producerAddress"`
	Version      uint64     `json:"protocolVersion"`
	Verification string     `json:"verificationState"`
	Source       string     `json:"source"`
}

// CheckMomentum follows the pinned wallet.VerifySignature semantics (standard
// Ed25519 over Hash.Bytes), without starting a wallet or consensus verifier.
// State changes, slot eligibility and account transactions are NOT validated.
func CheckMomentum(m *nom.Momentum, source string) (Header, error) {
	if m == nil || m.Version != 1 || m.ChainIdentifier != 1 || m.Height < 2 || m.PreviousHash.IsZero() || m.TimestampUnix == 0 || m.TimestampUnix > 1<<63-1 || len(m.Data) != 0 || len(m.Content) > 100 {
		return Header{}, errors.New("invalid momentum structure")
	}
	for _, h := range m.Content {
		if h == nil || h.Height == 0 || h.Hash.IsZero() {
			return Header{}, errors.New("invalid content header")
		}
	}
	if len(m.PublicKey) != ed25519.PublicKeySize || len(m.Signature) != ed25519.SignatureSize {
		return Header{}, errors.New("invalid signing key or signature size")
	}
	if m.ComputeHash() != m.Hash {
		return Header{}, errors.New("momentum hash mismatch")
	}
	if !ed25519.Verify(m.PublicKey, m.Hash.Bytes(), m.Signature) {
		return Header{}, errors.New("momentum signature invalid")
	}
	return Header{m.Hash, m.Height, m.PreviousHash, m.TimestampUnix, m.Producer().String(), m.Version, "signature_checked", source}, nil
}

// Preflight bounds decoded collection expansion before generic RLP decoding.
func Preflight(b []byte) error {
	if len(b) > MaxMessageSize {
		return errors.New("message too large")
	}
	nodes := 0
	var walk func([]byte, int) error
	walk = func(b []byte, depth int) error {
		if depth > 16 {
			return errors.New("RLP nesting limit")
		}
		for len(b) > 0 {
			nodes++
			if nodes > 4096 {
				return errors.New("RLP element limit")
			}
			kind, content, rest, err := rlp.Split(b)
			if err != nil {
				return err
			}
			if kind == rlp.List {
				if err = walk(content, depth+1); err != nil {
					return err
				}
			}
			b = rest
		}
		return nil
	}
	return walk(b, 0)
}
func Payload(msg p2p.Msg) ([]byte, error) {
	if msg.Size > MaxMessageSize {
		return nil, errors.New("message too large")
	}
	b, err := io.ReadAll(io.LimitReader(msg.Payload, int64(msg.Size)+1))
	if err != nil {
		return nil, err
	}
	if len(b) != int(msg.Size) {
		return nil, io.ErrUnexpectedEOF
	}
	return b, nil
}
func Decode(b []byte, v any) error {
	if err := Preflight(b); err != nil {
		return err
	}
	return rlp.DecodeBytes(b, v)
}
func Hashes(b []byte) ([]types.Hash, error) {
	var hashes []types.Hash
	if err := Decode(b, &hashes); err != nil {
		return nil, err
	}
	if len(hashes) > MaxHashes {
		return nil, errors.New("hash count limit")
	}
	return hashes, nil
}
func Full(b []byte) (Header, error) {
	// Account bodies stay raw and never enter the durable evidence stream.
	var d struct {
		Momentum      *nom.Momentum
		AccountBlocks rlp.RawValue
	}
	if err := Decode(b, &d); err != nil {
		return Header{}, err
	}
	content, rest, err := rlp.SplitList(d.AccountBlocks)
	if err != nil || len(rest) != 0 {
		return Header{}, errors.New("invalid account block list")
	}
	n, err := rlp.CountValues(content)
	if err != nil || n > 100 {
		return Header{}, errors.New("account block count limit")
	}
	return CheckMomentum(d.Momentum, "p2p")
}

func Exclusion(h Header, at time.Time, ready time.Time) string {
	if at.Sub(ready) < 5*time.Second {
		return "warmup"
	}
	stamp := time.Unix(int64(h.Timestamp), 0)
	if stamp.After(at.Add(10 * time.Second)) {
		return "future_slot"
	}
	if at.Sub(stamp) > 2*time.Minute {
		return "outside_live_window"
	}
	return ""
}
func ValidateRequest(code uint64, b []byte) error {
	switch code {
	case GetBlocksMsg:
		_, err := Hashes(b)
		return err
	case GetBlockHashesMsg:
		var v struct {
			Hash   types.Hash
			Amount uint64
		}
		if err := Decode(b, &v); err != nil {
			return err
		}
		if v.Amount > 2048 {
			return errors.New("history request too large")
		}
	case GetBlockHashesFromNumberMsg:
		var v struct{ Number, Amount uint64 }
		if err := Decode(b, &v); err != nil {
			return err
		}
		if v.Amount > 2048 {
			return errors.New("history request too large")
		}
	default:
		return fmt.Errorf("unexpected request code %d", code)
	}
	return nil
}
