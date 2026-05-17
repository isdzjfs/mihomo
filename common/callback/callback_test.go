package callback

import (
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

type testConn struct {
	writeErr                  error
	needHandshake             bool
	readerReplaceable         bool
	writerPossiblyReplaceable bool
	readerPossiblyReplaceable bool
	writeCalls                int
}

func (c *testConn) Read([]byte) (int, error) {
	return 0, io.EOF
}

func (c *testConn) Write(b []byte) (int, error) {
	c.writeCalls++
	if c.writeErr != nil {
		return 0, c.writeErr
	}
	return len(b), nil
}

func (c *testConn) ReadBuffer(*buf.Buffer) error {
	return io.EOF
}

func (c *testConn) WriteBuffer(buffer *buf.Buffer) error {
	_, err := c.Write(buffer.Bytes())
	return err
}

func (c *testConn) Close() error {
	return nil
}

func (c *testConn) LocalAddr() net.Addr {
	return testAddr("local")
}

func (c *testConn) RemoteAddr() net.Addr {
	return testAddr("remote")
}

func (c *testConn) SetDeadline(time.Time) error {
	return nil
}

func (c *testConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c *testConn) SetWriteDeadline(time.Time) error {
	return nil
}

func (c *testConn) Chains() C.Chain {
	return nil
}

func (c *testConn) ProviderChains() C.Chain {
	return nil
}

func (c *testConn) AppendToChains(C.ProxyAdapter) {}

func (c *testConn) RemoteDestination() string {
	return ""
}

func (c *testConn) NeedHandshake() bool {
	return c.needHandshake
}

func (c *testConn) ReaderReplaceable() bool {
	return c.readerReplaceable
}

func (c *testConn) WriterPossiblyReplaceable() bool {
	return c.writerPossiblyReplaceable
}

func (c *testConn) ReaderPossiblyReplaceable() bool {
	return c.readerPossiblyReplaceable
}

type testAddr string

func (a testAddr) Network() string {
	return string(a)
}

func (a testAddr) String() string {
	return string(a)
}

var _ C.Conn = (*testConn)(nil)

func TestFirstWriteCallbackConnDelegatesHandshakeState(t *testing.T) {
	inner := &testConn{
		needHandshake:             true,
		readerReplaceable:         false,
		writerPossiblyReplaceable: true,
		readerPossiblyReplaceable: true,
	}

	var callbacks []error
	conn := NewFirstWriteCallBackConn(inner, func(err error) {
		callbacks = append(callbacks, err)
	})

	if !N.NeedHandshake(conn) {
		t.Fatal("expected wrapper to delegate NeedHandshake before first write")
	}
	readerReplaceable, ok := conn.(interface{ ReaderReplaceable() bool })
	if !ok {
		t.Fatal("expected wrapper to expose ReaderReplaceable")
	}
	if readerReplaceable.ReaderReplaceable() {
		t.Fatal("expected wrapper to delegate ReaderReplaceable before first write")
	}

	writerPossiblyReplaceable, ok := conn.(interface{ WriterPossiblyReplaceable() bool })
	if !ok || !writerPossiblyReplaceable.WriterPossiblyReplaceable() {
		t.Fatal("expected wrapper to delegate WriterPossiblyReplaceable before first write")
	}
	readerPossiblyReplaceable, ok := conn.(interface{ ReaderPossiblyReplaceable() bool })
	if !ok || !readerPossiblyReplaceable.ReaderPossiblyReplaceable() {
		t.Fatal("expected wrapper to delegate ReaderPossiblyReplaceable")
	}
	writerReplaceable, ok := conn.(interface{ WriterReplaceable() bool })
	if !ok {
		t.Fatal("expected wrapper to expose WriterReplaceable")
	}
	if writerReplaceable.WriterReplaceable() {
		t.Fatal("wrapper should not be writer-replaceable before first write callback fires")
	}

	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("unexpected write error: %v", err)
	}
	if len(callbacks) != 1 || callbacks[0] != nil {
		t.Fatalf("expected one nil callback after first write, got %#v", callbacks)
	}
	if N.NeedHandshake(conn) {
		t.Fatal("wrapper should no longer need handshake after first write")
	}
	if writerPossiblyReplaceable.WriterPossiblyReplaceable() {
		t.Fatal("wrapper should not remain possibly writer-replaceable after first write")
	}
	if !writerReplaceable.WriterReplaceable() {
		t.Fatal("wrapper should become writer-replaceable after first write")
	}

	if _, err := conn.Write([]byte("again")); err != nil {
		t.Fatalf("unexpected second write error: %v", err)
	}
	if len(callbacks) != 1 {
		t.Fatalf("callback should only run for the first write, got %d calls", len(callbacks))
	}
}

func TestFirstWriteCallbackConnReportsFirstWriteError(t *testing.T) {
	writeErr := errors.New("write failed")
	inner := &testConn{writeErr: writeErr}

	var callbackErr error
	conn := NewFirstWriteCallBackConn(inner, func(err error) {
		callbackErr = err
	})

	buffer := buf.As([]byte("hello"))
	defer buffer.Release()
	err := conn.WriteBuffer(buffer)
	if !errors.Is(err, writeErr) {
		t.Fatalf("expected write error %v, got %v", writeErr, err)
	}
	if !errors.Is(callbackErr, writeErr) {
		t.Fatalf("expected callback error %v, got %v", writeErr, callbackErr)
	}
}
