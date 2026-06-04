package inbound

import (
	"net"
	"net/netip"

	"github.com/metacubex/mihomo/common/atomic"
	C "github.com/metacubex/mihomo/constant"
)

var skipAuthPrefixes = atomic.NewTypedValue([]netip.Prefix(nil))

func SetSkipAuthPrefixes(prefixes []netip.Prefix) {
	skipAuthPrefixes.Store(append([]netip.Prefix(nil), prefixes...))
}

func SkipAuthPrefixes() []netip.Prefix {
	return append([]netip.Prefix(nil), skipAuthPrefixes.Load()...)
}

func SkipAuthRemoteAddr(addr net.Addr) bool {
	m := C.Metadata{}
	if err := m.SetRemoteAddr(addr); err != nil {
		return false
	}
	return skipAuth(m.AddrPort().Addr())
}

func SkipAuthRemoteAddress(addr string) bool {
	m := C.Metadata{}
	if err := m.SetRemoteAddress(addr); err != nil {
		return false
	}
	return skipAuth(m.AddrPort().Addr())
}

func skipAuth(addr netip.Addr) bool {
	return prefixesContains(skipAuthPrefixes.Load(), addr)
}
