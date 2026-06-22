package trojan

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/transport/socks5"
)

type testConn struct {
	*bytes.Reader
}

func (c testConn) Write([]byte) (int, error) {
	return 0, io.ErrClosedPipe
}

func (c testConn) Close() error {
	return nil
}

func (c testConn) LocalAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c testConn) RemoteAddr() net.Addr {
	return &net.TCPAddr{}
}

func (c testConn) SetDeadline(time.Time) error {
	return nil
}

func (c testConn) SetReadDeadline(time.Time) error {
	return nil
}

func (c testConn) SetWriteDeadline(time.Time) error {
	return nil
}

type oneByteReadConn struct {
	testConn
}

func (c oneByteReadConn) Read(p []byte) (int, error) {
	if len(p) > 1 {
		p = p[:1]
	}
	return c.Reader.Read(p)
}

type blockingWriteConn struct {
	testConn
	started    chan struct{}
	release    chan struct{}
	active     atomic.Int32
	concurrent atomic.Bool
	mux        sync.Mutex
	raw        bytes.Buffer
}

func newBlockingWriteConn() *blockingWriteConn {
	return &blockingWriteConn{
		testConn: testConn{
			Reader: bytes.NewReader(nil),
		},
		started: make(chan struct{}, 2),
		release: make(chan struct{}),
	}
}

func (c *blockingWriteConn) Write(p []byte) (int, error) {
	if c.active.Add(1) > 1 {
		c.concurrent.Store(true)
	}
	c.started <- struct{}{}
	<-c.release
	defer c.active.Add(-1)

	c.mux.Lock()
	defer c.mux.Unlock()
	return c.raw.Write(p)
}

func trojanUDPFrame(addr socks5.Addr, payload, frameCRLF []byte) []byte {
	var raw bytes.Buffer
	raw.Write(addr)

	var header [4]byte
	binary.BigEndian.PutUint16(header[:2], uint16(len(payload)))
	copy(header[2:], frameCRLF)
	raw.Write(header[:])
	raw.Write(payload)

	return raw.Bytes()
}

func TestWritePacketReturnsPayloadLength(t *testing.T) {
	var raw bytes.Buffer
	addr := socks5.ParseAddr("127.0.0.1:53")
	payload := []byte("hello")

	n, err := WritePacket(&raw, addr, payload)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(payload) {
		t.Fatalf("written payload length = %d, want %d", n, len(payload))
	}
	if want := len(addr) + 2 + len(crlf) + len(payload); raw.Len() != want {
		t.Fatalf("encoded packet length = %d, want %d", raw.Len(), want)
	}
}

func TestWritePacketRejectsOversizedPayload(t *testing.T) {
	var raw bytes.Buffer
	payload := bytes.Repeat([]byte{0x42}, maxLength+1)

	n, err := WritePacket(&raw, socks5.ParseAddr("127.0.0.1:53"), payload)
	if !errors.Is(err, errPacketTooLarge) {
		t.Fatalf("expected oversized packet error, got %v", err)
	}
	if n != 0 {
		t.Fatalf("written payload length = %d, want 0", n)
	}
	if raw.Len() != 0 {
		t.Fatalf("oversized packet should not be written, got %d bytes", raw.Len())
	}
}

func TestPacketConnWriteToSerializesWrites(t *testing.T) {
	conn := newBlockingWriteConn()
	pc := NewPacketConn(conn)
	addr := &net.UDPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53}
	errCh := make(chan error, 2)

	go func() {
		_, err := pc.WriteTo([]byte("first"), addr)
		errCh <- err
	}()
	<-conn.started

	go func() {
		_, err := pc.WriteTo([]byte("second"), addr)
		errCh <- err
	}()

	select {
	case <-conn.started:
		close(conn.release)
		t.Fatal("second WriteTo entered before the first write completed")
	case <-time.After(50 * time.Millisecond):
	}

	close(conn.release)
	<-conn.started

	for i := 0; i < 2; i++ {
		if err := <-errCh; err != nil {
			t.Fatal(err)
		}
	}
	if conn.concurrent.Load() {
		t.Fatal("WriteTo calls overlapped")
	}
}

func TestReadPacketRejectsInvalidCRLF(t *testing.T) {
	raw := trojanUDPFrame(socks5.ParseAddr("127.0.0.1:53"), []byte("payload"), []byte{'\n', '\r'})

	_, _, _, err := ReadPacket(bytes.NewReader(raw), make([]byte, maxLength))
	if !errors.Is(err, errPacketInvalid) {
		t.Fatalf("expected invalid packet error, got %v", err)
	}
}

func TestPacketConnWaitReadFromRejectsInvalidCRLF(t *testing.T) {
	raw := trojanUDPFrame(socks5.ParseAddr("127.0.0.1:53"), []byte("payload"), []byte{'\n', '\r'})

	pc := NewPacketConn(testConn{Reader: bytes.NewReader(raw)})
	data, put, addr, err := pc.WaitReadFrom()
	if !errors.Is(err, errPacketInvalid) {
		t.Fatalf("expected invalid packet error, got %v", err)
	}
	if data != nil || put != nil || addr != nil {
		t.Fatal("expected no packet values after error")
	}
}

func TestPacketConnReadFromCompletesRemainChunk(t *testing.T) {
	payload := bytes.Repeat([]byte{0x42}, socks5.MaxAddrLen+3)
	pc := NewPacketConn(oneByteReadConn{
		testConn: testConn{
			Reader: bytes.NewReader(trojanUDPFrame(socks5.ParseAddr("127.0.0.1:53"), payload, crlf)),
		},
	})
	buf := make([]byte, socks5.MaxAddrLen)

	n, addr, err := pc.ReadFrom(buf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(buf) || !bytes.Equal(buf[:n], payload[:len(buf)]) {
		t.Fatalf("first read length = %d, want %d", n, len(buf))
	}
	if addr.String() != "127.0.0.1:53" {
		t.Fatalf("addr = %s, want 127.0.0.1:53", addr)
	}

	remainBuf := make([]byte, 3)
	n, addr, err = pc.ReadFrom(remainBuf)
	if err != nil {
		t.Fatal(err)
	}
	if n != len(remainBuf) || !bytes.Equal(remainBuf[:n], payload[len(buf):]) {
		t.Fatalf("remain read length = %d, want %d", n, len(remainBuf))
	}
	if addr.String() != "127.0.0.1:53" {
		t.Fatalf("addr = %s, want 127.0.0.1:53", addr)
	}
}

func TestPacketConnWaitReadFromRejectsOversizedLength(t *testing.T) {
	var raw bytes.Buffer
	raw.Write(socks5.ParseAddr("127.0.0.1:53"))

	var header [4]byte
	binary.BigEndian.PutUint16(header[:2], uint16(maxLength+1))
	copy(header[2:], crlf)
	raw.Write(header[:])

	pc := NewPacketConn(testConn{Reader: bytes.NewReader(raw.Bytes())})
	data, put, addr, err := pc.WaitReadFrom()
	if !errors.Is(err, errPacketInvalid) {
		t.Fatalf("expected oversized packet error, got %v", err)
	}
	if data != nil || put != nil || addr != nil {
		t.Fatal("expected no packet values after error")
	}
}
