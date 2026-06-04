package outboundgroup

import (
	"testing"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type urlTestProvider struct {
	name    string
	proxies []C.Proxy
	version uint32
}

func (p *urlTestProvider) Name() string { return p.name }

func (p *urlTestProvider) VehicleType() P.VehicleType { return P.Inline }

func (p *urlTestProvider) Type() P.ProviderType { return P.Proxy }

func (p *urlTestProvider) Initial() error { return nil }

func (p *urlTestProvider) Update() error { return nil }

func (p *urlTestProvider) Proxies() []C.Proxy { return p.proxies }

func (p *urlTestProvider) Count() int { return len(p.proxies) }

func (p *urlTestProvider) Touch() {}

func (p *urlTestProvider) HealthCheck() {}

func (p *urlTestProvider) Version() uint32 { return p.version }

func (p *urlTestProvider) RegisterHealthCheckTask(string, utils.IntRanges[uint16], string, string, string, uint) {
}

func (p *urlTestProvider) HealthCheckURL() string { return "" }

func TestURLTestDoesNotKeepCurrentNodeWhenAliveStateFlapsAfterScan(t *testing.T) {
	const testURL = "https://www.gstatic.com/generate_204"

	deadA := &parserTestProxy{
		name:    "A",
		typ:     C.Socks5,
		aliveFn: func(string) bool { return false },
		delay:   100,
	}

	aliveReads := 0
	flappingB := &parserTestProxy{
		name: "B",
		typ:  C.Socks5,
		aliveFn: func(string) bool {
			aliveReads++
			return aliveReads > 1
		},
		delay: 10,
	}

	aliveC := &parserTestProxy{
		name:  "C",
		typ:   C.Socks5,
		delay: 50,
	}

	group, err := NewURLTest(
		&GroupCommonOption{Name: "auto", URL: testURL},
		aliveC,
		[]P.ProxyProvider{&urlTestProvider{
			name:    "provider",
			proxies: []C.Proxy{deadA, flappingB, aliveC},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	group.fastNode = flappingB

	got := group.fast(false)
	if got.Name() != aliveC.Name() {
		t.Fatalf("expected URLTest to switch away from dead snapshot node, got %s", got.Name())
	}
}
