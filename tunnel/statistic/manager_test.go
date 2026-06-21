package statistic

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"

	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

type testNetConn struct {
	read bytes.Reader
}

func newTestNetConn(read []byte) *testNetConn {
	return &testNetConn{read: *bytes.NewReader(read)}
}

func (c *testNetConn) Read(b []byte) (int, error) {
	return c.read.Read(b)
}

func (c *testNetConn) Write(b []byte) (int, error) {
	return len(b), nil
}

func (c *testNetConn) Close() error {
	return nil
}

func (c *testNetConn) LocalAddr() net.Addr {
	return testAddr("127.0.0.1:1080")
}

func (c *testNetConn) RemoteAddr() net.Addr {
	return testAddr("203.0.113.1:443")
}

func (c *testNetConn) SetDeadline(time.Time) error {
	return nil
}

func (c *testNetConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *testNetConn) SetWriteDeadline(time.Time) error {
	return nil
}

type testAddr string

func (a testAddr) Network() string {
	return "tcp"
}

func (a testAddr) String() string {
	return string(a)
}

type testConn struct {
	N.ExtendedConn
}

func newTestConn(read []byte) *testConn {
	return &testConn{ExtendedConn: N.NewExtendedConn(newTestNetConn(read))}
}

func (c *testConn) Chains() C.Chain {
	return C.Chain{"proxy"}
}

func (c *testConn) ProviderChains() C.Chain {
	return nil
}

func (c *testConn) AppendToChains(C.ProxyAdapter) {
}

func (c *testConn) RemoteDestination() string {
	return "203.0.113.1:443"
}

type panicInfoTracker struct {
	*testConn
	id string
}

func (t *panicInfoTracker) ID() string {
	return t.id
}

func (t *panicInfoTracker) Close() error {
	return nil
}

func (t *panicInfoTracker) Info() *TrackerInfo {
	panic("Info should not be called by O(1) traffic queries")
}

func TestTrackerAddsProxyTraffic(t *testing.T) {
	manager := &Manager{}
	tracker := NewTCPTracker(newTestConn([]byte("download")), manager, &C.Metadata{}, nil, 3, 5, true)

	readBuf := make([]byte, 4)
	n, err := tracker.Read(readBuf)
	if err != nil && err != io.EOF {
		t.Fatalf("Read() error = %v", err)
	}
	if n != 4 {
		t.Fatalf("Read() bytes = %d, want 4", n)
	}

	n, err = tracker.Write([]byte("upload"))
	if err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	if n != 6 {
		t.Fatalf("Write() bytes = %d, want 6", n)
	}

	if up, down := manager.TotalTraffic(true); up != 9 || down != 9 {
		t.Fatalf("proxy total = (%d, %d), want (9, 9)", up, down)
	}
	if up, down := manager.TotalTraffic(false); up != 9 || down != 9 {
		t.Fatalf("global total = (%d, %d), want (9, 9)", up, down)
	}
	if up, down := tracker.UploadTotal.Load(), tracker.DownloadTotal.Load(); up != 9 || down != 9 {
		t.Fatalf("tracker total = (%d, %d), want (9, 9)", up, down)
	}
}

func TestNowTrafficOnlyProxyDoesNotReadTrackers(t *testing.T) {
	manager := &Manager{}
	manager.Join(&panicInfoTracker{testConn: newTestConn(nil), id: "panic"})

	manager.PushUploaded(10)
	manager.PushDownloaded(20)
	manager.pushProxyUploaded(3)
	manager.pushProxyDownloaded(4)
	manager.updateBlip()

	if up, down := manager.NowTraffic(true); up != 3 || down != 4 {
		t.Fatalf("proxy now traffic = (%d, %d), want (3, 4)", up, down)
	}
	if up, down := manager.NowTraffic(false); up != 10 || down != 20 {
		t.Fatalf("global now traffic = (%d, %d), want (10, 20)", up, down)
	}
}

func TestTrackerCloseKeepsProxyTotal(t *testing.T) {
	manager := &Manager{}
	tracker := NewTCPTracker(newTestConn(nil), manager, &C.Metadata{}, nil, 0, 0, true)

	if _, err := tracker.Write([]byte("persist")); err != nil {
		t.Fatalf("Write() error = %v", err)
	}
	id := tracker.ID()
	if manager.Get(id) == nil {
		t.Fatal("tracker was not joined")
	}
	if err := tracker.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	if manager.Get(id) != nil {
		t.Fatal("tracker was not removed after Close()")
	}
	if up, down := manager.TotalTraffic(true); up != 7 || down != 0 {
		t.Fatalf("proxy total after close = (%d, %d), want (7, 0)", up, down)
	}
}

func TestGlobalAndProxyTrafficAreSeparated(t *testing.T) {
	manager := &Manager{}

	manager.PushUploaded(100)
	manager.PushDownloaded(200)
	manager.PushUploaded(7)
	manager.PushDownloaded(11)
	manager.pushProxyUploaded(7)
	manager.pushProxyDownloaded(11)

	if up, down := manager.TotalTraffic(false); up != 107 || down != 211 {
		t.Fatalf("global total = (%d, %d), want (107, 211)", up, down)
	}
	if up, down := manager.TotalTraffic(true); up != 7 || down != 11 {
		t.Fatalf("proxy total = (%d, %d), want (7, 11)", up, down)
	}

	manager.updateBlip()
	if up, down := manager.NowTraffic(false); up != 107 || down != 211 {
		t.Fatalf("global now traffic = (%d, %d), want (107, 211)", up, down)
	}
	if up, down := manager.NowTraffic(true); up != 7 || down != 11 {
		t.Fatalf("proxy now traffic = (%d, %d), want (7, 11)", up, down)
	}

	manager.ResetStatistic()
	if up, down := manager.TotalTraffic(false); up != 0 || down != 0 {
		t.Fatalf("global total after reset = (%d, %d), want (0, 0)", up, down)
	}
	if up, down := manager.TotalTraffic(true); up != 0 || down != 0 {
		t.Fatalf("proxy total after reset = (%d, %d), want (0, 0)", up, down)
	}
}
