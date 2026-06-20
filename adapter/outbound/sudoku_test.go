package outbound

import (
	"testing"

	"github.com/metacubex/mihomo/transport/sudoku"
)

func TestShouldUseHTTPMaskMultiplexRequiresEnabledTunnelMode(t *testing.T) {
	tests := []struct {
		name     string
		mux      string
		mode     string
		disabled bool
		want     bool
	}{
		{name: "stream enabled", mux: "on", mode: "stream", want: true},
		{name: "poll enabled", mux: "on", mode: "poll", want: true},
		{name: "auto enabled", mux: "on", mode: "auto", want: true},
		{name: "http mask disabled", mux: "on", mode: "stream", disabled: true},
		{name: "legacy mode", mux: "on", mode: "legacy"},
		{name: "websocket mode", mux: "on", mode: "ws"},
		{name: "mux auto", mux: "auto", mode: "stream"},
		{name: "mux off", mux: "off", mode: "stream"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := sudoku.DefaultConfig()
			cfg.HTTPMaskMultiplex = tt.mux
			cfg.HTTPMaskMode = tt.mode
			cfg.DisableHTTPMask = tt.disabled

			if got := shouldUseHTTPMaskMultiplex(cfg); got != tt.want {
				t.Fatalf("shouldUseHTTPMaskMultiplex() = %t, want %t", got, tt.want)
			}
		})
	}
}
