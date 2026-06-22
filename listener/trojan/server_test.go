package trojan

import (
	"errors"
	"net"
	"strings"
	"testing"
)

type closeErrListener struct {
	err error
}

func (l closeErrListener) Accept() (net.Conn, error) {
	return nil, net.ErrClosed
}

func (l closeErrListener) Close() error {
	return l.err
}

func (l closeErrListener) Addr() net.Addr {
	return &net.TCPAddr{}
}

func TestReadCRLF(t *testing.T) {
	tests := []struct {
		name string
		data string
		want bool
	}{
		{name: "valid", data: "\r\n", want: true},
		{name: "wrong order", data: "\n\r", want: false},
		{name: "short", data: "\r", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := readCRLF(strings.NewReader(test.data)); got != test.want {
				t.Fatalf("readCRLF() = %v, want %v", got, test.want)
			}
		})
	}
}

func TestListenerCloseJoinsListenerErrors(t *testing.T) {
	errA := errors.New("first close error")
	errB := errors.New("second close error")
	listener := &Listener{
		listeners: []net.Listener{
			closeErrListener{err: errA},
			closeErrListener{err: errB},
		},
	}

	err := listener.Close()
	if !errors.Is(err, errA) || !errors.Is(err, errB) {
		t.Fatalf("Close() error = %v, want both listener errors", err)
	}
	if !listener.closed.Load() {
		t.Fatal("Close() did not mark listener closed")
	}
}
