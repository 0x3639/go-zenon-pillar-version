package reference

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"time"

	"github.com/0x3639/go-zenon-pillar-version/internal/netguard"
	"github.com/0x3639/go-zenon-pillar-version/internal/protocol"
	"github.com/gorilla/websocket"
	"github.com/zenon-network/go-zenon/chain/nom"
	"github.com/zenon-network/go-zenon/common/types"
)

const MaxResponseBytes = 256 * 1024

type Client struct{ URLs []string }

func New(urls []string) (*Client, error) {
	if len(urls) == 0 || len(urls) > 3 {
		return nil, errors.New("configure 1–3 RPC references")
	}
	for _, s := range urls {
		u, e := url.Parse(s)
		if e != nil || u.Scheme != "wss" || u.Hostname() == "" || u.User != nil || u.Fragment != "" {
			return nil, errors.New("RPC endpoints must be secure wss URLs without credentials")
		}
	}
	return &Client{URLs: urls}, nil
}
func (c *Client) Call(ctx context.Context, endpoint, method string, params any, out any) error {
	d := websocket.Dialer{HandshakeTimeout: 5 * time.Second, NetDialContext: netguard.DialContext, Proxy: nil}
	ctx, cancel := context.WithTimeout(ctx, 8*time.Second)
	defer cancel()
	ws, resp, err := d.DialContext(ctx, endpoint, nil)
	if resp != nil && resp.Body != nil {
		defer resp.Body.Close()
	}
	if err != nil {
		return err
	}
	defer ws.Close()
	stop := context.AfterFunc(ctx, func() { ws.Close() })
	defer stop()
	ws.SetReadLimit(MaxResponseBytes)
	deadline, _ := ctx.Deadline()
	_ = ws.SetReadDeadline(deadline)
	_ = ws.SetWriteDeadline(deadline)
	if err = ws.WriteJSON(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params}); err != nil {
		return err
	}
	var reply struct {
		ID     int             `json:"id"`
		Result json.RawMessage `json:"result"`
		Error  json.RawMessage `json:"error"`
	}
	if err = ws.ReadJSON(&reply); err != nil {
		return err
	}
	if reply.ID != 1 {
		return errors.New("RPC response ID mismatch")
	}
	if len(reply.Error) > 0 && string(reply.Error) != "null" {
		return errors.New("RPC returned an error")
	}
	if len(reply.Result) == 0 || string(reply.Result) == "null" {
		return errors.New("RPC record unavailable")
	}
	return json.Unmarshal(reply.Result, out)
}
func (c *Client) Frontier(ctx context.Context) (protocol.Header, error) {
	var last error
	for _, u := range c.URLs {
		var m nom.Momentum
		if e := c.Call(ctx, u, "ledger.getFrontierMomentum", []any{}, &m); e != nil {
			last = e
			continue
		}
		h, e := protocol.CheckMomentum(&m, u)
		if e != nil {
			last = e
			continue
		}
		if protocol.Exclusion(h, time.Now(), time.Now().Add(-time.Minute)) != "" {
			last = errors.New("reference frontier is not recent")
			continue
		}
		h.Verification = "rpc_observed"
		return h, nil
	}
	return protocol.Header{}, fmt.Errorf("no usable reference frontier: %w", last)
}
func (c *Client) Resolve(ctx context.Context, hash types.Hash) (protocol.Header, error) {
	var last error
	for _, u := range c.URLs {
		var m nom.Momentum
		if e := c.Call(ctx, u, "ledger.getMomentumByHash", []any{hash.String()}, &m); e != nil {
			last = e
			continue
		}
		h, e := protocol.CheckMomentum(&m, u)
		if e != nil {
			last = e
			continue
		}
		if h.Hash != hash {
			last = errors.New("RPC hash mismatch")
			continue
		}
		h.Verification = "rpc_observed"
		return h, nil
	}
	return protocol.Header{}, fmt.Errorf("unresolved hash: %w", last)
}
