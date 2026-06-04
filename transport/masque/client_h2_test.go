package masque

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"github.com/metacubex/quic-go/quicvarint"
)

func TestH2ReceiveDatagramRejectsOversizedCapsule(t *testing.T) {
	frame := quicvarint.Append(nil, h2DatagramCapsuleType)
	frame = quicvarint.Append(frame, maxH2DatagramPayloadLen+1)

	stream := &h2DatagramStream{
		responseBody: io.NopCloser(bytes.NewReader(frame)),
	}

	_, err := stream.ReceiveDatagram(context.Background())
	if err == nil || !strings.Contains(err.Error(), "exceeds maximum") {
		t.Fatalf("expected oversized capsule error, got %v", err)
	}
}
