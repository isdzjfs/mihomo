package resource

import (
	"context"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/metacubex/mihomo/common/utils"
	P "github.com/metacubex/mihomo/constant/provider"
)

type fetcherTestVehicle struct {
	path   string
	remote []byte
	reads  atomic.Int32
	writes atomic.Int32
}

func (v *fetcherTestVehicle) Read(context.Context, utils.HashType) ([]byte, utils.HashType, error) {
	v.reads.Add(1)
	buf := append([]byte(nil), v.remote...)
	return buf, utils.MakeHash(buf), nil
}

func (v *fetcherTestVehicle) Write(buf []byte) error {
	v.writes.Add(1)
	return os.WriteFile(v.path, buf, fileMode)
}

func (v *fetcherTestVehicle) Path() string { return v.path }
func (v *fetcherTestVehicle) Url() string  { return "https://example.com/rules.txt" }
func (v *fetcherTestVehicle) Proxy() string {
	return ""
}
func (v *fetcherTestVehicle) Type() P.VehicleType { return P.HTTP }

type errorBundleFile struct {
	data []byte
	read bool
}

func (f *errorBundleFile) Read(p []byte) (int, error) {
	if f.read {
		return 0, io.EOF
	}
	f.read = true
	return copy(p, f.data), io.ErrUnexpectedEOF
}

func (f *errorBundleFile) Close() error { return nil }

func (f *errorBundleFile) Stat() (fs.FileInfo, error) {
	return bundleFileInfo{name: "bundle-entry", size: int64(len(f.data)), modTime: time.Unix(100, 0)}, nil
}

type bundleFileInfo struct {
	name    string
	size    int64
	modTime time.Time
}

func (fi bundleFileInfo) Name() string       { return fi.name }
func (fi bundleFileInfo) Size() int64        { return fi.size }
func (fi bundleFileInfo) Mode() fs.FileMode  { return 0o644 }
func (fi bundleFileInfo) ModTime() time.Time { return fi.modTime }
func (fi bundleFileInfo) IsDir() bool        { return false }
func (fi bundleFileInfo) Sys() any           { return nil }

func TestFetcherInitialFallsBackToRemoteWhenBundleReadFails(t *testing.T) {
	cachePath := filepath.Join(t.TempDir(), "rules.txt")
	vehicle := &fetcherTestVehicle{
		path:   cachePath,
		remote: []byte("remote"),
	}
	fetcher := NewFetcher[string](
		"rules",
		0,
		vehicle,
		func() (fs.File, error) {
			return &errorBundleFile{data: []byte("partial")}, nil
		},
		func(buf []byte) (string, error) {
			return string(buf), nil
		},
		nil,
	)

	got, err := fetcher.Initial()
	if err != nil {
		t.Fatalf("Initial() error = %v", err)
	}
	if got != "remote" {
		t.Fatalf("Initial() = %q, want %q", got, "remote")
	}
	if reads := vehicle.reads.Load(); reads != 1 {
		t.Fatalf("remote reads = %d, want 1", reads)
	}
	if writes := vehicle.writes.Load(); writes != 1 {
		t.Fatalf("cache writes = %d, want 1", writes)
	}
	cached, err := os.ReadFile(cachePath)
	if err != nil {
		t.Fatalf("ReadFile(cache) error = %v", err)
	}
	if string(cached) != "remote" {
		t.Fatalf("cached content = %q, want %q", string(cached), "remote")
	}
}
