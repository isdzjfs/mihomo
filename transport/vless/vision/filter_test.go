package vision

import (
	"encoding/binary"
	"testing"
)

func TestFilterTLSServerHelloWithMalformedSessionIDDoesNotPanic(t *testing.T) {
	buffer := make([]byte, 79)
	buffer[0] = 0x16
	buffer[1] = 0x03
	buffer[2] = 0x03
	binary.BigEndian.PutUint16(buffer[3:5], uint16(len(buffer)-tlsRecordHeaderLen))
	buffer[5] = tlsHandshakeTypeServerHello
	buffer[6] = 0
	buffer[7] = 0
	buffer[8] = byte(len(buffer) - tlsRecordHeaderLen - tlsHandshakeHeaderLen)
	buffer[43] = tlsMaxSessionIDLen + 1

	conn := &Conn{packetsToFilter: 1}
	conn.FilterTLS(buffer)
}
