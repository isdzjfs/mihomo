package provider

import (
	"context"
	"errors"
	"path/filepath"
	"testing"

	"github.com/metacubex/mihomo/common/utils"
	C "github.com/metacubex/mihomo/constant"
	P "github.com/metacubex/mihomo/constant/provider"
)

type initialErrorVehicle struct {
	path       string
	typ        P.VehicleType
	err        error
	readCalled bool
}

func (v *initialErrorVehicle) Read(context.Context, utils.HashType) ([]byte, utils.HashType, error) {
	v.readCalled = true
	return nil, utils.HashType{}, v.err
}

func (v *initialErrorVehicle) Write([]byte) error { return nil }

func (v *initialErrorVehicle) Path() string { return v.path }

func (v *initialErrorVehicle) Url() string { return "https://example.com/provider.yaml" }

func (v *initialErrorVehicle) Proxy() string { return "" }

func (v *initialErrorVehicle) Type() P.VehicleType { return v.typ }

func TestProxySetProviderInitialAllowsHTTPPullError(t *testing.T) {
	forbiddenErr := errors.New("403 Forbidden")
	vehicle := &initialErrorVehicle{
		path: filepath.Join(t.TempDir(), "provider.yaml"),
		typ:  P.HTTP,
		err:  forbiddenErr,
	}
	provider, err := NewProxySetProvider(
		"blocked",
		0,
		nil,
		func([]byte) ([]C.Proxy, error) {
			t.Fatal("parser should not be called when the HTTP pull fails")
			return nil, nil
		},
		vehicle,
		NewHealthCheck(nil, "", 0, 0, true, nil),
	)
	if err != nil {
		t.Fatalf("NewProxySetProvider() error = %v", err)
	}
	defer provider.Close()

	if err := provider.Initial(); err != nil {
		t.Fatalf("Initial() error = %v, want nil", err)
	}
	if !vehicle.readCalled {
		t.Fatal("expected Initial() to try the HTTP vehicle")
	}
	if count := provider.Count(); count != 0 {
		t.Fatalf("provider count = %d, want 0", count)
	}
}

func TestProxySetProviderInitialKeepsFilePullErrorFatal(t *testing.T) {
	forbiddenErr := errors.New("403 Forbidden")
	vehicle := &initialErrorVehicle{
		path: filepath.Join(t.TempDir(), "provider.yaml"),
		typ:  P.File,
		err:  forbiddenErr,
	}
	provider, err := NewProxySetProvider(
		"blocked-file",
		0,
		nil,
		func([]byte) ([]C.Proxy, error) {
			t.Fatal("parser should not be called when the file read fails")
			return nil, nil
		},
		vehicle,
		NewHealthCheck(nil, "", 0, 0, true, nil),
	)
	if err != nil {
		t.Fatalf("NewProxySetProvider() error = %v", err)
	}
	defer provider.Close()

	if err := provider.Initial(); !errors.Is(err, forbiddenErr) {
		t.Fatalf("Initial() error = %v, want %v", err, forbiddenErr)
	}
}
