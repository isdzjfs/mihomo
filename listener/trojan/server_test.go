package trojan

import (
	"strings"
	"testing"
)

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
