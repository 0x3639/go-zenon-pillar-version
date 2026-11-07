// Package netguard pins a validated public address for each dial, avoiding
// DNS re-resolution by the transport and rejecting special-use destinations.
package netguard

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/zenon-network/go-zenon/p2p/discover"
)

var blocked = prefixes([]string{
	"0.0.0.0/8", "10.0.0.0/8", "100.64.0.0/10", "127.0.0.0/8", "169.254.0.0/16", "172.16.0.0/12",
	"192.0.0.0/24", "192.0.2.0/24", "192.88.99.0/24", "192.168.0.0/16", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "224.0.0.0/4", "240.0.0.0/4",
	"2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20",
})

func prefixes(s []string) []netip.Prefix {
	p := make([]netip.Prefix, len(s))
	for i, v := range s {
		p[i] = netip.MustParsePrefix(v)
	}
	return p
}
func Public(ip netip.Addr) bool {
	if !ip.IsValid() || ip.Zone() != "" {
		return false
	}
	ip = ip.Unmap()
	if !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	// Only currently allocated IPv6 global unicast; excludes NAT64, ULA,
	// mapped special addresses and translation paths to metadata services.
	if ip.Is6() && !netip.MustParsePrefix("2000::/3").Contains(ip) {
		return false
	}
	for _, p := range blocked {
		if p.Contains(ip) {
			return false
		}
	}
	return true
}

type Candidate struct {
	ID   discover.NodeID
	Host string
	Port uint16
}

func Parse(raw string) (Candidate, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Scheme != "enode" || u.User == nil || u.Path != "" || u.RawQuery != "" || u.Fragment != "" {
		return Candidate{}, errors.New("seed must be enode://<128 hex public key>@host:port")
	}
	if _, ok := u.User.Password(); ok {
		return Candidate{}, errors.New("seed password forbidden")
	}
	id, err := discover.HexID(u.User.Username())
	if err != nil {
		return Candidate{}, err
	}
	if _, err = id.Pubkey(); err != nil {
		return Candidate{}, err
	}
	p, err := strconv.ParseUint(u.Port(), 10, 16)
	if err != nil || p == 0 || u.Hostname() == "" {
		return Candidate{}, errors.New("explicit valid seed port required")
	}
	if strings.Contains(u.Hostname(), "%") {
		return Candidate{}, errors.New("scoped IP forbidden")
	}
	return Candidate{ID: id, Host: u.Hostname(), Port: uint16(p)}, nil
}

func Resolve(ctx context.Context, host string) ([]netip.Addr, error) {
	if a, err := netip.ParseAddr(host); err == nil {
		if !Public(a) {
			return nil, errors.New("non-public destination")
		}
		return []netip.Addr{a.Unmap()}, nil
	}
	ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 || len(ips) > 32 {
		return nil, errors.New("invalid DNS answer count")
	}
	for i, ip := range ips {
		if !Public(ip) {
			return nil, errors.New("DNS includes non-public destination")
		}
		ips[i] = ip.Unmap()
	}
	return ips, nil
}

func DialIP(ctx context.Context, ip netip.Addr, port string) (net.Conn, error) {
	if !Public(ip) {
		return nil, errors.New("non-public destination")
	}
	d := net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}
	c, err := d.DialContext(ctx, "tcp", net.JoinHostPort(ip.String(), port))
	if err != nil {
		return nil, err
	}
	a, ok := c.RemoteAddr().(*net.TCPAddr)
	if !ok || a.AddrPort().Addr().Unmap() != ip.Unmap() || strconv.Itoa(a.Port) != port {
		c.Close()
		return nil, errors.New("connected endpoint mismatch")
	}
	return c, nil
}

func DialContext(ctx context.Context, network, address string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return nil, err
	}
	ips, err := Resolve(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolve public endpoint: %w", err)
	}
	return DialIP(ctx, ips[0], port)
}
