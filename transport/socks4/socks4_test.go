package socks4

import (
	"bytes"
	"errors"
	"testing"
)

func TestReadUntilNullRejectsLongField(t *testing.T) {
	input := bytes.NewReader(bytes.Repeat([]byte{'a'}, maxNullTerminatedFieldLen+1))
	_, err := readUntilNull(input)
	if !errors.Is(err, errFieldTooLong) {
		t.Fatalf("expected field-too-long error, got %v", err)
	}
}

func TestReadUntilNullAllowsMaximumFieldLength(t *testing.T) {
	input := append(bytes.Repeat([]byte{'a'}, maxNullTerminatedFieldLen), 0)
	got, err := readUntilNull(bytes.NewReader(input))
	if err != nil {
		t.Fatalf("readUntilNull returned error: %v", err)
	}
	if len(got) != maxNullTerminatedFieldLen {
		t.Fatalf("expected length %d, got %d", maxNullTerminatedFieldLen, len(got))
	}
}
