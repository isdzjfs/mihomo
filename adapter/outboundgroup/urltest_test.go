package outboundgroup

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/adapter/outbound"
	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
	"github.com/metacubex/mihomo/tunnel/statistic"
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

type urlTestTracker struct {
	id     string
	chain  C.Chain
	closed int32
}

func (t *urlTestTracker) ID() string { return t.id }

func (t *urlTestTracker) Close() error {
	atomic.StoreInt32(&t.closed, 1)
	statistic.DefaultManager.Leave(t)
	return nil
}

func (t *urlTestTracker) Info() *statistic.TrackerInfo { return nil }

func (t *urlTestTracker) Chains() C.Chain { return t.chain }

func (t *urlTestTracker) ProviderChains() C.Chain { return nil }

func (t *urlTestTracker) AppendToChains(adapter C.ProxyAdapter) {
	t.chain = append(t.chain, adapter.Name())
}

func (t *urlTestTracker) RemoteDestination() string { return "" }

func (t *urlTestTracker) Closed() bool {
	return atomic.LoadInt32(&t.closed) == 1
}

type dialURLTestProxy struct {
	parserTestProxy
	dialFn func(context.Context, *C.Metadata) (C.Conn, error)
}

func (p *dialURLTestProxy) DialContext(ctx context.Context, metadata *C.Metadata) (C.Conn, error) {
	if p.dialFn != nil {
		return p.dialFn(ctx, metadata)
	}
	return p.parserTestProxy.DialContext(ctx, metadata)
}

func (p *dialURLTestProxy) Adapter() C.ProxyAdapter { return p }

func newURLTestConn(t *testing.T, proxy C.ProxyAdapter) C.Conn {
	t.Helper()
	local, remote := net.Pipe()
	t.Cleanup(func() {
		_ = local.Close()
		_ = remote.Close()
	})
	return outbound.NewConn(local, proxy)
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

func TestURLTestClosesConnectionsWhenCurrentNodeTurnsDeadWithoutAlternative(t *testing.T) {
	const testURL = "https://www.gstatic.com/generate_204"

	deadA := &parserTestProxy{
		name:    "A",
		typ:     C.Socks5,
		aliveFn: func(string) bool { return false },
	}
	deadB := &parserTestProxy{
		name:    "B",
		typ:     C.Socks5,
		aliveFn: func(string) bool { return false },
	}

	group, err := NewURLTest(
		&GroupCommonOption{Name: "auto", URL: testURL},
		deadA,
		[]P.ProxyProvider{&urlTestProvider{
			name:    "provider",
			proxies: []C.Proxy{deadA, deadB},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	group.fastNode = deadA

	tracker := &urlTestTracker{
		id:    "stale-current",
		chain: C.Chain{deadA.Name(), group.Name()},
	}
	statistic.DefaultManager.Join(tracker)
	t.Cleanup(func() { statistic.DefaultManager.Leave(tracker) })

	got := group.fastWithHealthCheck(false, false)
	if got.Name() != deadA.Name() {
		t.Fatalf("expected URLTest to keep first dead node as last resort, got %s", got.Name())
	}
	if !tracker.Closed() {
		t.Fatal("expected URLTest to close connections for the stale dead current node")
	}
}

func TestURLTestDiscardsConnectionDialedByStaleNode(t *testing.T) {
	const testURL = "https://www.gstatic.com/generate_204"

	var group *URLTest
	var aAlive int32 = 1
	var staleA *dialURLTestProxy
	aliveB := &dialURLTestProxy{
		parserTestProxy: parserTestProxy{
			name:  "B",
			typ:   C.Socks5,
			delay: 20,
		},
	}
	aliveB.dialFn = func(context.Context, *C.Metadata) (C.Conn, error) {
		return newURLTestConn(t, aliveB), nil
	}
	staleA = &dialURLTestProxy{
		parserTestProxy: parserTestProxy{
			name: "A",
			typ:  C.Socks5,
			aliveFn: func(string) bool {
				return atomic.LoadInt32(&aAlive) == 1
			},
			delay: 10,
		},
	}
	staleA.dialFn = func(context.Context, *C.Metadata) (C.Conn, error) {
		atomic.StoreInt32(&aAlive, 0)
		group.fastSingle.Reset()
		group.setFastNode(aliveB, false)
		return newURLTestConn(t, staleA), nil
	}

	var err error
	group, err = NewURLTest(
		&GroupCommonOption{Name: "auto", URL: testURL},
		staleA,
		[]P.ProxyProvider{&urlTestProvider{
			name:    "provider",
			proxies: []C.Proxy{staleA, aliveB},
		}},
	)
	if err != nil {
		t.Fatal(err)
	}
	group.fastNode = staleA

	conn, err := group.DialContext(context.Background(), &C.Metadata{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })

	if got := conn.Chains().Last(); got != aliveB.Name() {
		t.Fatalf("expected URLTest to redial through fresh node %s, got chain %v", aliveB.Name(), conn.Chains())
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
