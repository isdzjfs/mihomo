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

type needHandshakeConn interface {
	NeedHandshake() bool
}

type readerReplaceableConn interface {
	ReaderReplaceable() bool
}

type writerPossiblyReplaceableConn interface {
	WriterPossiblyReplaceable() bool
}

type readerPossiblyReplaceableConn interface {
	ReaderPossiblyReplaceable() bool
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
	if inner, ok := c.Conn.(readerReplaceableConn); ok {
		return inner.ReaderReplaceable()
	}
	return true
}

func (c *firstWriteCallBackConn) NeedHandshake() bool {
	// Keep the wrapper transparent until the first write, so protocols with
	// lazy handshakes such as VLESS still expose their handshake state to Relay.
	if c.written {
		return false
	}
	if inner, ok := c.Conn.(needHandshakeConn); ok {
		return inner.NeedHandshake()
	}
	return false
}

func (c *firstWriteCallBackConn) WriterPossiblyReplaceable() bool {
	// VLESS Vision can replace its writer during early writes. Delegating this
	// signal lets Relay preserve that path while this wrapper observes failures.
	if c.written {
		return false
	}
	if inner, ok := c.Conn.(writerPossiblyReplaceableConn); ok {
		return inner.WriterPossiblyReplaceable()
	}
	return false
}

func (c *firstWriteCallBackConn) ReaderPossiblyReplaceable() bool {
	if inner, ok := c.Conn.(readerPossiblyReplaceableConn); ok {
		return inner.ReaderPossiblyReplaceable()
	}
	return false
}

var _ N.ExtendedConn = (*firstWriteCallBackConn)(nil)

func NewFirstWriteCallBackConn(c C.Conn, callback func(error)) C.Conn {
	return &firstWriteCallBackConn{
		Conn:     c,
		callback: callback,
		written:  false,
	}
}
