package vless

import (
	"net"
	"testing"

	N "github.com/metacubex/mihomo/common/net"
)

func TestRecvResponseReturnsErrorForTruncatedAddon(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()

	errCh := make(chan error, 1)
	go func() {
		defer server.Close()
		_, err := server.Write([]byte{Version, 3, 1})
		errCh <- err
	}()

	vc := &Conn{ExtendedConn: N.NewExtendedConn(client)}
	if err := vc.recvResponse(); err == nil {
		t.Fatal("recvResponse() error = nil, want truncated addon error")
	}
	if err := <-errCh; err != nil {
		t.Fatalf("server write failed: %v", err)
	}
}
