package outboundgroup

import (
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type urlTestProvider struct {
	name         string
	proxies      []C.Proxy
	version      uint32
	healthChecks int32
}

func (p *urlTestProvider) Name() string { return p.name }

func (p *urlTestProvider) VehicleType() P.VehicleType { return P.Inline }

func (p *urlTestProvider) Type() P.ProviderType { return P.Proxy }

func (p *urlTestProvider) Initial() error { return nil }

func (p *urlTestProvider) Update() error { return nil }

func (p *urlTestProvider) Proxies() []C.Proxy { return p.proxies }

func (p *urlTestProvider) Count() int { return len(p.proxies) }

func (p *urlTestProvider) Touch() {}

func (p *urlTestProvider) HealthCheck() {
	atomic.AddInt32(&p.healthChecks, 1)
}

func (p *urlTestProvider) Version() uint32 { return p.version }

func (p *urlTestProvider) RegisterHealthCheckTask(string, utils.IntRanges[uint16], string, string, string, uint) {
}

func (p *urlTestProvider) HealthCheckURL() string { return "" }

func (p *urlTestProvider) HealthCheckCount() int32 {
	return atomic.LoadInt32(&p.healthChecks)
}

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

func TestURLTestManualSelectedDeadNodeFallsBackToAliveNode(t *testing.T) {
	const testURL = "https://www.gstatic.com/generate_204"

	deadA := &parserTestProxy{
		name:    "A",
		typ:     C.Socks5,
		aliveFn: func(string) bool { return false },
		delay:   10,
	}
	aliveB := &parserTestProxy{
		name:  "B",
		typ:   C.Socks5,
		delay: 50,
	}

	group, err := NewURLTest(
		&GroupCommonOption{Name: "auto", URL: testURL},
		aliveB,
		[]P.ProxyProvider{&urlTestProvider{
			name:    "provider",
			proxies: []C.Proxy{deadA, aliveB},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	group.ForceSet(deadA.Name())

	got := group.fast(false)
	if got.Name() != aliveB.Name() {
		t.Fatalf("expected URLTest to avoid manually selected dead node, got %s", got.Name())
	}
}

func TestURLTestPreheatDoesNotTriggerHealthCheckWhenAllNodesDead(t *testing.T) {
	const testURL = "https://www.gstatic.com/generate_204"

	deadA := &parserTestProxy{name: "A", typ: C.Socks5, aliveFn: func(string) bool { return false }}
	deadB := &parserTestProxy{name: "B", typ: C.Socks5, aliveFn: func(string) bool { return false }}
	provider := &urlTestProvider{
		name:    "provider",
		proxies: []C.Proxy{deadA, deadB},
	}

	group, err := NewURLTest(
		&GroupCommonOption{Name: "auto", URL: testURL},
		deadA,
		[]P.ProxyProvider{provider},
	)
	if err != nil {
		t.Fatal(err)
	}

	got := group.fastWithHealthCheck(false, false)
	if got.Name() != deadA.Name() {
		t.Fatalf("expected URLTest to keep first dead node as last resort, got %s", got.Name())
	}
	if calls := provider.HealthCheckCount(); calls != 0 {
		t.Fatalf("expected preheat path not to trigger health check, got %d calls", calls)
	}
}

func TestFallbackManualSelectedDeadNodeFallsBackToAliveNode(t *testing.T) {
	const testURL = "https://www.gstatic.com/generate_204"

	deadA := &parserTestProxy{
		name:    "A",
		typ:     C.Socks5,
		aliveFn: func(string) bool { return false },
	}
	aliveB := &parserTestProxy{name: "B", typ: C.Socks5}

	group, err := NewFallback(
		&GroupCommonOption{Name: "fallback", URL: testURL},
		aliveB,
		[]P.ProxyProvider{&urlTestProvider{
			name:    "provider",
			proxies: []C.Proxy{deadA, aliveB},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	group.ForceSet(deadA.Name())

	got := group.findAliveProxy(false)
	if got.Name() != aliveB.Name() {
		t.Fatalf("expected Fallback to avoid manually selected dead node, got %s", got.Name())
	}
	if selected := group.getSelected(); selected != "" {
		t.Fatalf("expected Fallback to clear dead manual selection, got %s", selected)
	}
}
