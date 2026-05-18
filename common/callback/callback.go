package callback

import (
	"github.com/metacubex/mihomo/common/buf"
	N "github.com/metacubex/mihomo/common/net"
	C "github.com/metacubex/mihomo/constant"
)

type firstWriteCallBackConn struct {
	C.Conn
	callback func(error)
	written  bool
}

func (c *firstWriteCallBackConn) Write(b []byte) (n int, err error) {
	defer func() {
		if !c.written {
			c.written = true
			c.callback(err)
		}
	}()
	return c.Conn.Write(b)
}

func (c *firstWriteCallBackConn) WriteBuffer(buffer *buf.Buffer) (err error) {
	defer func() {
		if !c.written {
			c.written = true
			c.callback(err)
		}
	}()
	return c.Conn.WriteBuffer(buffer)
}

func (c *firstWriteCallBackConn) Upstream() any {
	return c.Conn
}

func (c *firstWriteCallBackConn) WriterReplaceable() bool {
	return c.written
}

func (c *firstWriteCallBackConn) ReaderReplaceable() bool {
	// Delegate to the underlying connection to preserve its read-path semantics.
	// For example, VLESS returns false until its response header has been parsed,
	// and returning true here unconditionally would cause the relay to bypass that
	// logic, breaking the protocol.
	type readerReplaceableConn interface {
		ReaderReplaceable() bool
	}
	if inner, ok := c.Conn.(readerReplaceableConn); ok {
		return inner.ReaderReplaceable()
	}
	return true
}

var _ N.ExtendedConn = (*firstWriteCallBackConn)(nil)

func NewFirstWriteCallBackConn(c C.Conn, callback func(error)) C.Conn {
	return &firstWriteCallBackConn{
		Conn:     c,
		callback: callback,
		written:  false,
	}
}
