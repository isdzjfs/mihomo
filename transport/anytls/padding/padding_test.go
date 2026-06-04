package padding

import (
	"bytes"
	"fmt"
	"strings"
	"testing"
)

func TestNewPaddingFactoryRejectsOversizedRawScheme(t *testing.T) {
	raw := bytes.Repeat([]byte{'x'}, MaxRawSchemeSize+1)
	if NewPaddingFactory(raw) != nil {
		t.Fatal("expected oversized raw scheme to be rejected")
	}
}

func TestNewPaddingFactoryRejectsOversizedRange(t *testing.T) {
	raw := []byte(fmt.Sprintf("stop=2\n1=%d-%d", MaxRecordPayloadSize+1, MaxRecordPayloadSize+1))
	if NewPaddingFactory(raw) != nil {
		t.Fatal("expected oversized padding range to be rejected")
	}
}

func TestNewPaddingFactoryRejectsTooManyRecords(t *testing.T) {
	ranges := strings.Repeat("1-1,", MaxRecordsPerPacketRule) + "1-1"
	raw := []byte("stop=2\n1=" + ranges)
	if NewPaddingFactory(raw) != nil {
		t.Fatal("expected packet rule with too many records to be rejected")
	}
}

func TestPaddingFactoryAllowsMaximumRecordSize(t *testing.T) {
	raw := []byte(fmt.Sprintf("stop=2\n1=%d-%d", MaxRecordPayloadSize, MaxRecordPayloadSize))
	factory := NewPaddingFactory(raw)
	if factory == nil {
		t.Fatal("expected maximum record size to be accepted")
	}
	sizes := factory.GenerateRecordPayloadSizes(1)
	if len(sizes) != 1 || sizes[0] != MaxRecordPayloadSize {
		t.Fatalf("unexpected generated sizes: %v", sizes)
	}
}
