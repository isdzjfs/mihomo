package geodata

import (
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/metacubex/http"
	"github.com/metacubex/http/httptest"
)

func TestDownloadToPathRespectsDownloadAllowed(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = w.Write([]byte("geo"))
	}))
	defer server.Close()

	restore := SetDownloadAllowed(false)
	t.Cleanup(restore)

	err := downloadToPath(server.URL, filepath.Join(t.TempDir(), "geo.dat"))
	if !errors.Is(err, ErrDownloadDeferred) {
		t.Fatalf("downloadToPath() error = %v, want %v", err, ErrDownloadDeferred)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("requests = %d, want 0", got)
	}
}
