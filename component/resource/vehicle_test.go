package resource

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func TestHTTPVehicleReadRetriesUnexpectedEOF(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if attempts.Add(1) == 1 {
			w.Header().Set("Content-Length", "5")
			_, _ = w.Write([]byte("abc"))
			if flusher, ok := w.(http.Flusher); ok {
				flusher.Flush()
			}
			return
		}
		_, _ = w.Write([]byte("hello"))
	}))
	defer server.Close()

	vehicle := NewHTTPVehicle(server.URL, "", "", nil, time.Second, 0)
	buf, hash, err := vehicle.Read(context.Background(), utils.HashType{})
	if err != nil {
		t.Fatalf("Read() error = %v", err)
	}
	if string(buf) != "hello" {
		t.Fatalf("Read() buf = %q, want %q", string(buf), "hello")
	}
	if !hash.Equal(utils.MakeHash([]byte("hello"))) {
		t.Fatal("Read() returned unexpected hash")
	}
	if got := attempts.Load(); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}

func TestHTTPVehicleReadRespectsDownloadAllowed(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte("hello"))
	}))
	defer server.Close()

	restore := SetDownloadAllowed(false)
	t.Cleanup(restore)

	vehicle := NewHTTPVehicle(server.URL, "", "", nil, time.Second, 0)
	_, _, err := vehicle.Read(context.Background(), utils.HashType{})
	if !errors.Is(err, ErrDownloadDeferred) {
		t.Fatalf("Read() error = %v, want %v", err, ErrDownloadDeferred)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
}
