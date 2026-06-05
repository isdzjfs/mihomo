package updater

import (
	"context"
	"errors"
	"os"
	"sync/atomic"
	"testing"
	"time"

	C "github.com/metacubex/mihomo/constant"
)

func TestRunGeoUpdaterContinuesAfterInitialUpdateError(t *testing.T) {
	oldHomeDir := C.Path.HomeDir()
	C.SetHomeDir(t.TempDir())
	t.Cleanup(func() {
		C.SetHomeDir(oldHomeDir)
	})

	mmdbPath := C.Path.MMDB()
	if err := os.WriteFile(mmdbPath, []byte("stale"), 0o644); err != nil {
		t.Fatalf("WriteFile() error = %v", err)
	}
	staleTime := time.Now().Add(-2 * time.Hour)
	if err := os.Chtimes(mmdbPath, staleTime, staleTime); err != nil {
		t.Fatalf("Chtimes() error = %v", err)
	}

	oldUpdateGeoDatabasesForRunner := updateGeoDatabasesForRunner
	var attempts atomic.Int32
	updateGeoDatabasesForRunner = func() error {
		attempts.Add(1)
		return errors.New("download failed")
	}
	t.Cleanup(func() {
		updateGeoDatabasesForRunner = oldUpdateGeoDatabasesForRunner
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		runGeoUpdater(ctx, 1)
		close(done)
	}()

	deadline := time.After(time.Second)
	for attempts.Load() == 0 {
		select {
		case <-done:
			t.Fatal("runGeoUpdater returned before initial update ran")
		case <-deadline:
			t.Fatal("timed out waiting for initial update")
		case <-time.After(10 * time.Millisecond):
		}
	}

	select {
	case <-done:
		t.Fatal("runGeoUpdater returned after initial update error")
	case <-time.After(50 * time.Millisecond):
	}

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("runGeoUpdater did not stop after context cancellation")
	}
}
