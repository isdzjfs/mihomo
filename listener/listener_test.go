package listener

import "testing"

func TestShouldCloseBeforeListenForOverlappingAddresses(t *testing.T) {
	tests := []struct {
		name string
		old  string
		new  string
		want bool
	}{
		{name: "loopback to wildcard", old: "127.0.0.1:7890", new: ":7890", want: true},
		{name: "wildcard to loopback", old: ":7890", new: "127.0.0.1:7890", want: true},
		{name: "same address ignored", old: "127.0.0.1:7890", new: "127.0.0.1:7890", want: false},
		{name: "different ports", old: "127.0.0.1:7890", new: ":7891", want: false},
		{name: "different concrete hosts", old: "127.0.0.1:7890", new: "192.0.2.1:7890", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := shouldCloseBeforeListen(tt.old, tt.new)
			if got != tt.want {
				t.Fatalf("shouldCloseBeforeListen(%q, %q) = %t, want %t", tt.old, tt.new, got, tt.want)
			}
		})
	}
}
