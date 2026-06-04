package outbound

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"net"
	"net/netip"
	"os"
	"sync/atomic"
	"testing"

	"github.com/metacubex/mihomo/dns"

	wireguard "github.com/metacubex/sing-wireguard"
	M "github.com/metacubex/sing/common/metadata"
	wgTun "github.com/metacubex/wireguard-go/tun"
)

type lifecycleTestDevice struct {
	events chan wgTun.Event
	closed int32
}

func newLifecycleTestDevice() *lifecycleTestDevice {
	return &lifecycleTestDevice{events: make(chan wgTun.Event, 1)}
}

func (d *lifecycleTestDevice) File() *os.File { return nil }

func (d *lifecycleTestDevice) Read(_ [][]byte, _ []int, _ int) (int, error) {
	return 0, os.ErrClosed
}

func (d *lifecycleTestDevice) Write(_ [][]byte, _ int) (int, error) {
	return 0, os.ErrClosed
}

func (d *lifecycleTestDevice) MTU() (int, error) { return 1280, nil }

func (d *lifecycleTestDevice) Name() (string, error) { return "lifecycle-test", nil }

func (d *lifecycleTestDevice) Events() <-chan wgTun.Event { return d.events }

func (d *lifecycleTestDevice) Close() error {
	if atomic.AddInt32(&d.closed, 1) == 1 {
		close(d.events)
	}
	return nil
}

func (d *lifecycleTestDevice) BatchSize() int { return 1 }

func (d *lifecycleTestDevice) DialContext(context.Context, string, M.Socksaddr) (net.Conn, error) {
	return nil, os.ErrClosed
}

func (d *lifecycleTestDevice) ListenPacket(context.Context, M.Socksaddr) (net.PacketConn, error) {
	return nil, os.ErrClosed
}

func (d *lifecycleTestDevice) Start() error { return nil }

func (d *lifecycleTestDevice) Inet4Address() netip.Addr {
	return netip.MustParseAddr("10.0.0.1")
}

func (d *lifecycleTestDevice) Inet6Address() netip.Addr { return netip.Addr{} }

func (d *lifecycleTestDevice) RegisterForward(wireguard.ForwardOptions) error { return nil }

func (d *lifecycleTestDevice) closeCount() int32 {
	return atomic.LoadInt32(&d.closed)
}

func installLifecycleStackDevice(t *testing.T) *lifecycleTestDevice {
	t.Helper()

	device := newLifecycleTestDevice()
	oldFactory := newWireGuardStackDevice
	newWireGuardStackDevice = func([]netip.Prefix, uint32) (wireguard.Device, error) {
		return device, nil
	}
	t.Cleanup(func() {
		newWireGuardStackDevice = oldFactory
	})
	return device
}

func installFailingNameServerParser(t *testing.T) {
	t.Helper()

	oldParser := dns.ParseNameServer
	dns.ParseNameServer = func([]string) ([]dns.NameServer, error) {
		return nil, errors.New("forced nameserver parse failure")
	}
	t.Cleanup(func() {
		dns.ParseNameServer = oldParser
	})
}

func testWireGuardKey() string {
	return base64.StdEncoding.EncodeToString(make([]byte, 32))
}

func testMasqueKeys(t *testing.T) (string, string) {
	t.Helper()

	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	publicKey, err := x509.MarshalPKIXPublicKey(&key.PublicKey)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(privateKey), base64.StdEncoding.EncodeToString(publicKey)
}

func TestWireGuardConstructorRollsBackDeviceOnLaterFailure(t *testing.T) {
	device := installLifecycleStackDevice(t)
	installFailingNameServerParser(t)

	adapter, err := NewWireGuard(WireGuardOption{
		Name:       "wg-lifecycle",
		Ip:         "10.0.0.2/32",
		PrivateKey: testWireGuardKey(),
		WireGuardPeerOption: WireGuardPeerOption{
			Server:    "example.com",
			Port:      51820,
			PublicKey: testWireGuardKey(),
		},
		RemoteDnsResolve: true,
		Dns:              []string{"rcode://success"},
	})
	if err == nil {
		t.Fatal("expected constructor failure")
	}
	if adapter != nil {
		t.Fatalf("expected nil adapter on failure, got %#v", adapter)
	}
	if count := device.closeCount(); count != 1 {
		t.Fatalf("expected rollback to close WireGuard device once, got %d; err=%v", count, err)
	}
}

func TestMasqueConstructorRollsBackDeviceOnLaterFailure(t *testing.T) {
	device := installLifecycleStackDevice(t)
	installFailingNameServerParser(t)
	privateKey, publicKey := testMasqueKeys(t)

	adapter, err := NewMasque(MasqueOption{
		Name:             "masque-lifecycle",
		Server:           "example.com",
		Port:             443,
		Ip:               "10.0.0.2/32",
		PrivateKey:       privateKey,
		PublicKey:        publicKey,
		Network:          "h2",
		RemoteDnsResolve: true,
		Dns:              []string{"rcode://success"},
	})
	if err == nil {
		t.Fatal("expected constructor failure")
	}
	if adapter != nil {
		t.Fatalf("expected nil adapter on failure, got %#v", adapter)
	}
	if count := device.closeCount(); count != 1 {
		t.Fatalf("expected rollback to close Masque device once, got %d; err=%v", count, err)
	}
}
