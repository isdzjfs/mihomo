package inbound

import (
	"net"
	"net/netip"

	"github.com/metacubex/mihomo/common/atomic"
	C "github.com/metacubex/mihomo/constant"
)

var lanAllowedIPs = atomic.NewTypedValue([]netip.Prefix(nil))
var lanDisAllowedIPs = atomic.NewTypedValue([]netip.Prefix(nil))

func SetAllowedIPs(prefixes []netip.Prefix) {
	lanAllowedIPs.Store(append([]netip.Prefix(nil), prefixes...))
}

func SetDisAllowedIPs(prefixes []netip.Prefix) {
	lanDisAllowedIPs.Store(append([]netip.Prefix(nil), prefixes...))
}

func AllowedIPs() []netip.Prefix {
	return append([]netip.Prefix(nil), lanAllowedIPs.Load()...)
}

func DisAllowedIPs() []netip.Prefix {
	return append([]netip.Prefix(nil), lanDisAllowedIPs.Load()...)
}

func IsRemoteAddrDisAllowed(addr net.Addr) bool {
	m := C.Metadata{}
	if err := m.SetRemoteAddr(addr); err != nil {
		return false
	}
	ipAddr := m.AddrPort().Addr()
	if ipAddr.IsValid() {
		return isAllowed(ipAddr) && !isDisAllowed(ipAddr)
	}
	return false
}

func isAllowed(addr netip.Addr) bool {
	return prefixesContains(lanAllowedIPs.Load(), addr)
}

func isDisAllowed(addr netip.Addr) bool {
	return prefixesContains(lanDisAllowedIPs.Load(), addr)
}
