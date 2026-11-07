package netguard

import (
	"context"
	"net/netip"
	"testing"
)

func TestPublicAddressPolicy(t *testing.T) {
	for _, s := range []string{"127.0.0.1", "10.1.2.3", "100.100.100.200", "169.254.169.254", "192.168.0.1", "172.16.1.1", "198.18.0.1", "192.0.2.1", "198.51.100.1", "203.0.113.1", "240.1.2.3", "224.0.0.1", "0.0.0.0", "::", "::1", "::ffff:127.0.0.1", "::ffff:169.254.169.254", "fe80::1", "fc00::1", "ff02::1", "64:ff9b::a9fe:a9fe", "2002:7f00:1::", "2001:db8::1", "2001::1", "3fff::1"} {
		if Public(netip.MustParseAddr(s)) {
			t.Errorf("accepted %s", s)
		}
	}
	for _, s := range []string{"8.8.8.8", "51.81.209.53", "2606:4700:4700::1111", "::ffff:8.8.8.8"} {
		if !Public(netip.MustParseAddr(s)) {
			t.Errorf("rejected %s", s)
		}
	}
	if _, e := Resolve(context.Background(), "::ffff:169.254.169.254"); e == nil {
		t.Fatal("metadata address resolved")
	}
}
func TestSeedParsingRejectsBadInputs(t *testing.T) {
	for _, s := range []string{"http://example.com", "enode://bad@8.8.8.8:35995", "enode://@8.8.8.8:0", "%%%"} {
		if _, e := Parse(s); e == nil {
			t.Errorf("accepted %s", s)
		}
	}
}
