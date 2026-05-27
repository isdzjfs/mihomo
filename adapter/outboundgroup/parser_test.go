package outboundgroup

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type parserTestProxy struct {
	name    string
	typ     C.AdapterType
	aliveFn func(string) bool
	delay   uint16
}

func (p *parserTestProxy) Name() string { return p.name }

func (p *parserTestProxy) Type() C.AdapterType { return p.typ }

func (p *parserTestProxy) Addr() string { return "" }

func (p *parserTestProxy) SupportUDP() bool { return true }

func (p *parserTestProxy) ProxyInfo() C.ProxyInfo { return C.ProxyInfo{} }

func (p *parserTestProxy) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"name": p.name})
}

func (p *parserTestProxy) DialContext(context.Context, *C.Metadata) (C.Conn, error) {
	return nil, C.ErrNotSupport
}

func (p *parserTestProxy) ListenPacketContext(context.Context, *C.Metadata) (C.PacketConn, error) {
	return nil, C.ErrNotSupport
}

func (p *parserTestProxy) SupportUOT() bool { return false }

func (p *parserTestProxy) IsL3Protocol(*C.Metadata) bool { return false }

func (p *parserTestProxy) Unwrap(*C.Metadata, bool) C.Proxy { return nil }

func (p *parserTestProxy) Close() error { return nil }

func (p *parserTestProxy) Adapter() C.ProxyAdapter { return p }

func (p *parserTestProxy) AliveForTestUrl(url string) bool {
	if p.aliveFn != nil {
		return p.aliveFn(url)
	}
	return true
}

func (p *parserTestProxy) DelayHistory() []C.DelayHistory { return nil }

func (p *parserTestProxy) ExtraDelayHistories() map[string]C.ProxyState { return nil }

func (p *parserTestProxy) LastDelayForTestUrl(string) uint16 {
	if p.delay != 0 {
		return p.delay
	}
	return 1
}

func (p *parserTestProxy) URLTest(context.Context, string, utils.IntRanges[uint16]) (uint16, error) {
	return 1, nil
}

func TestParseProxyGroupFiltersExcludedProxiesBeforeCompatibleProvider(t *testing.T) {
	proxyMap := map[string]C.Proxy{
		"COMPATIBLE":  &parserTestProxy{name: "COMPATIBLE", typ: C.Compatible},
		"HK normal":   &parserTestProxy{name: "HK normal", typ: C.Socks5},
		"HK YepFast":  &parserTestProxy{name: "HK YepFast", typ: C.Socks5},
		"HK hysteria": &parserTestProxy{name: "HK hysteria", typ: C.Hysteria2},
	}
	providersMap := map[string]P.ProxyProvider{}

	_, err := ParseProxyGroup(
		map[string]any{
			"name":           "auto",
			"type":           "url-test",
			"include-all":    true,
			"filter":         "HK",
			"exclude-filter": "YepFast",
			"exclude-type":   "Hysteria|Hysteria2",
		},
		proxyMap,
		providersMap,
		[]string{"HK normal", "HK YepFast", "HK hysteria"},
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}

	pd, ok := providersMap["auto"]
	if !ok {
		t.Fatal("expected compatible provider to be registered")
	}

	var got []string
	for _, proxy := range pd.Proxies() {
		got = append(got, proxy.Name())
	}
	want := []string{"HK normal"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("unexpected compatible provider proxies: got %v, want %v", got, want)
	}
}
