package provider

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
)

type healthCheckTestProxy struct {
	name  string
	typ   C.AdapterType
	calls atomic.Int32
}

func (p *healthCheckTestProxy) Name() string { return p.name }

func (p *healthCheckTestProxy) Type() C.AdapterType { return p.typ }

func (p *healthCheckTestProxy) Addr() string { return "" }

func (p *healthCheckTestProxy) SupportUDP() bool { return true }

func (p *healthCheckTestProxy) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{} }

func (p *healthCheckTestProxy) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"name": p.name})
}

func (p *healthCheckTestProxy) DialContext(context.Context, *C.Metadata) (C.Conn, error) {
	return nil, C.ErrNotSupport
}

func (p *healthCheckTestProxy) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	return nil, C.ErrNotSupport
}

func (p *healthCheckTestProxy) SupportUOT() bool { return false }

func (p *healthCheckTestProxy) IsL3Protocol(*C.Metadata) bool { return false }

func (p *healthCheckTestProxy) Unwrap(*C.Metadata, bool) C.Proxy { return nil }

func (p *healthCheckTestProxy) Close() error { return nil }

func (p *healthCheckTestProxy) Adapter() C.ProxyAdapter { return p }

func (p *healthCheckTestProxy) AliveForTestUrl(string) bool { return true }

func (p *healthCheckTestProxy) DelayHistory() []C.DelayHistory { return nil }

func (p *healthCheckTestProxy) ExtraDelayHistories() map[string]C.ProxyState { return nil }

func (p *healthCheckTestProxy) LastDelayForTestUrl(string) uint16 { return 1 }

func (p *healthCheckTestProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	p.calls.Add(1)
	return 1, nil
}

func TestHealthCheckExtraTaskAppliesExcludeFilterAndType(t *testing.T) {
	keep := &healthCheckTestProxy{name: "HK normal", typ: C.Socks5}
	excludedByName := &healthCheckTestProxy{name: "HK YepFast", typ: C.Socks5}
	excludedByType := &healthCheckTestProxy{name: "HK hysteria2", typ: C.Hysteria2}
	filterMiss := &healthCheckTestProxy{name: "US normal", typ: C.Socks5}

	hc := NewHealthCheck(
		[]C.Proxy{keep, excludedByName, excludedByType, filterMiss},
		"",
		1000,
		0,
		true,
		nil,
	)
	hc.registerHealthCheckTask("https://example.com/generate_204", nil, "HK", "YepFast", "Hysteria|Hysteria2", 0)
	hc.check()

	if calls := keep.calls.Load(); calls != 1 {
		t.Fatalf("expected kept proxy to be checked once, got %d", calls)
	}
	if calls := excludedByName.calls.Load(); calls != 0 {
		t.Fatalf("expected name-excluded proxy to be skipped, got %d checks", calls)
	}
	if calls := excludedByType.calls.Load(); calls != 0 {
		t.Fatalf("expected type-excluded proxy to be skipped, got %d checks", calls)
	}
	if calls := filterMiss.calls.Load(); calls != 0 {
		t.Fatalf("expected filter-miss proxy to be skipped, got %d checks", calls)
	}
}

func TestHealthCheckExtraTaskSameURLUsesUnionOfTaskCandidates(t *testing.T) {
	hkHysteria := &healthCheckTestProxy{name: "HK hysteria2", typ: C.Hysteria2}
	jpHysteria := &healthCheckTestProxy{name: "JP hysteria2", typ: C.Hysteria2}

	hc := NewHealthCheck(
		[]C.Proxy{hkHysteria, jpHysteria},
		"",
		1000,
		0,
		true,
		nil,
	)
	hc.registerHealthCheckTask("https://example.com/generate_204", nil, "HK", "", "Hysteria2", 0)
	hc.registerHealthCheckTask("https://example.com/generate_204", nil, "JP", "", "", 0)
	hc.check()

	if calls := hkHysteria.calls.Load(); calls != 0 {
		t.Fatalf("expected Hysteria2 excluded by HK task to be skipped, got %d checks", calls)
	}
	if calls := jpHysteria.calls.Load(); calls != 1 {
		t.Fatalf("expected JP task without type exclusion to check Hysteria2 once, got %d checks", calls)
	}
}
