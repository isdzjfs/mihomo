package trojan

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
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
