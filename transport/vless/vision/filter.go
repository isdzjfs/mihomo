package vision

import (
	"bytes"
	"encoding/binary"

	"github.com/metacubex/mihomo/log"
)

var (
	tls13SupportedVersions  = []byte{0x00, 0x2b, 0x00, 0x02, 0x03, 0x04}
	tlsClientHandshakeStart = []byte{0x16, 0x03}
	tlsServerHandshakeStart = []byte{0x16, 0x03, 0x03}
	tlsApplicationDataStart = []byte{0x17, 0x03, 0x03}

	tls13CipherSuiteMap = map[uint16]string{
		0x1301: "TLS_AES_128_GCM_SHA256",
		0x1302: "TLS_AES_256_GCM_SHA384",
		0x1303: "TLS_CHACHA20_POLY1305_SHA256",
		0x1304: "TLS_AES_128_CCM_SHA256",
		0x1305: "TLS_AES_128_CCM_8_SHA256",
	}
)

const (
	tlsHandshakeTypeClientHello byte = 0x01
	tlsHandshakeTypeServerHello byte = 0x02

	tlsRecordHeaderLen     = 5
	tlsHandshakeHeaderLen  = 4
	tlsServerHelloFixedLen = 2 + 32 + 1
	tlsMaxSessionIDLen     = 32
)

func (vc *Conn) FilterTLS(buffer []byte) (index int) {
	if vc.packetsToFilter <= 0 {
		return 0
	}
	lenP := len(buffer)
	vc.packetsToFilter--
	if index = bytes.Index(buffer, tlsServerHandshakeStart); index != -1 {
		if lenP > index+tlsRecordHeaderLen {
			if buffer[index] == 22 && buffer[index+1] == 3 && buffer[index+2] == 3 {
				vc.isTLS = true
				if buffer[index+tlsRecordHeaderLen] == tlsHandshakeTypeServerHello {
					//log.Debugln("isTLS12orAbove")
					vc.remainingServerHello = int(binary.BigEndian.Uint16(buffer[index+3:index+5])) + tlsRecordHeaderLen
					vc.isTLS12orAbove = true
					if cipher, ok := parseServerHelloCipher(buffer, index); ok {
						vc.cipher = cipher
					}
				}
			}
		}
	} else if index = bytes.Index(buffer, tlsClientHandshakeStart); index != -1 {
		if lenP > index+5 && buffer[index+5] == tlsHandshakeTypeClientHello {
			vc.isTLS = true
		}
	}

	if vc.remainingServerHello > 0 {
		end := int(vc.remainingServerHello)
		i := index
		if i < 0 {
			i = 0
		}
		if i+end > lenP {
			end = lenP
			vc.remainingServerHello -= end - i
		} else {
			vc.remainingServerHello -= end
			end += i
		}
		if bytes.Contains(buffer[i:end], tls13SupportedVersions) {
			// TLS 1.3 Client Hello
			cs, ok := tls13CipherSuiteMap[vc.cipher]
			if ok && cs != "TLS_AES_128_CCM_8_SHA256" {
				vc.enableXTLS = true
			}
			log.Debugln("XTLS Vision found TLS 1.3, packetLength=%d， CipherSuite=%s", lenP, cs)
			vc.packetsToFilter = 0
			return
		} else if vc.remainingServerHello <= 0 {
			log.Debugln("XTLS Vision found TLS 1.2, packetLength=%d", lenP)
			vc.packetsToFilter = 0
			return
		}
		log.Debugln("XTLS Vision found inconclusive server hello, packetLength=%d, remainingServerHelloBytes=%d", lenP, vc.remainingServerHello)
	}
	if vc.packetsToFilter <= 0 {
		log.Debugln("XTLS Vision stop filtering")
	}
	return
}

func parseServerHelloCipher(buffer []byte, index int) (uint16, bool) {
	if index < 0 || len(buffer) < index+tlsRecordHeaderLen+tlsHandshakeHeaderLen+tlsServerHelloFixedLen {
		return 0, false
	}

	recordLen := int(binary.BigEndian.Uint16(buffer[index+3 : index+5]))
	handshakeStart := index + tlsRecordHeaderLen
	if buffer[handshakeStart] != tlsHandshakeTypeServerHello {
		return 0, false
	}
	handshakeLen := int(buffer[handshakeStart+1])<<16 | int(buffer[handshakeStart+2])<<8 | int(buffer[handshakeStart+3])
	if handshakeLen < tlsServerHelloFixedLen || handshakeLen+tlsHandshakeHeaderLen > recordLen {
		return 0, false
	}

	cursor := handshakeStart + tlsHandshakeHeaderLen + 2 + 32
	if cursor >= len(buffer) {
		return 0, false
	}
	sessionIDLen := int(buffer[cursor])
	if sessionIDLen > tlsMaxSessionIDLen {
		return 0, false
	}
	cursor++
	if tlsHandshakeHeaderLen+tlsServerHelloFixedLen+sessionIDLen+2 > recordLen || cursor+sessionIDLen+2 > len(buffer) {
		return 0, false
	}
	cursor += sessionIDLen
	return binary.BigEndian.Uint16(buffer[cursor : cursor+2]), true
}
