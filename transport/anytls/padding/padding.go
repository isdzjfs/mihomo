package padding

import (
	"crypto/md5"
	"crypto/rand"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/metacubex/mihomo/transport/anytls/util"
)

const (
	CheckMark = -1

	// Padding records are camouflage, not payload. These limits keep peer-supplied
	// schemes from turning into large allocations or bandwidth amplification.
	MaxRawSchemeSize        = 4 * 1024
	MaxRecordPayloadSize    = 16 * 1024
	MaxPaddingStop          = 64
	MaxRecordsPerPacketRule = 64
)

var DefaultPaddingScheme = []byte(`stop=8
0=30-30
1=100-400
2=400-500,c,500-1000,c,500-1000,c,500-1000,c,500-1000
3=9-9,500-1000
4=500-1000
5=500-1000
6=500-1000
7=500-1000`)

type PaddingFactory struct {
	scheme    util.StringMap
	RawScheme []byte
	Stop      uint32
	Md5       string
}

func UpdatePaddingScheme(rawScheme []byte, to *atomic.Pointer[PaddingFactory]) bool {
	if p := NewPaddingFactory(rawScheme); p != nil {
		to.Store(p)
		return true
	}
	return false
}

func NewPaddingFactory(rawScheme []byte) *PaddingFactory {
	if len(rawScheme) == 0 || len(rawScheme) > MaxRawSchemeSize {
		return nil
	}
	p := &PaddingFactory{
		RawScheme: append([]byte(nil), rawScheme...),
		Md5:       fmt.Sprintf("%x", md5.Sum(rawScheme)),
	}
	scheme := util.StringMapFromBytes(rawScheme)
	if len(scheme) == 0 {
		return nil
	}
	if stop, err := strconv.Atoi(scheme["stop"]); err == nil {
		if stop < 0 || stop > MaxPaddingStop {
			return nil
		}
		p.Stop = uint32(stop)
	} else {
		return nil
	}
	if !validatePaddingScheme(scheme, p.Stop) {
		return nil
	}
	p.scheme = scheme
	return p
}

func (p *PaddingFactory) GenerateRecordPayloadSizes(pkt uint32) (pktSizes []int) {
	if s, ok := p.scheme[strconv.Itoa(int(pkt))]; ok {
		sRanges := strings.Split(s, ",")
		for _, sRange := range sRanges {
			if minValue, maxValue, ok := parsePaddingRange(sRange); ok {
				if minValue == maxValue {
					pktSizes = append(pktSizes, minValue)
				} else {
					i, err := rand.Int(rand.Reader, big.NewInt(int64(maxValue-minValue)))
					if err != nil {
						continue
					}
					pktSizes = append(pktSizes, int(i.Int64())+minValue)
				}
			} else if sRange == "c" {
				pktSizes = append(pktSizes, CheckMark)
			}
		}
	}
	return
}

func validatePaddingScheme(scheme util.StringMap, stop uint32) bool {
	for key, value := range scheme {
		if key == "stop" {
			continue
		}
		pkt, err := strconv.Atoi(key)
		if err != nil || pkt < 0 || uint32(pkt) >= stop {
			return false
		}
		ranges := strings.Split(value, ",")
		if len(ranges) == 0 || len(ranges) > MaxRecordsPerPacketRule {
			return false
		}
		for _, sRange := range ranges {
			if sRange == "c" {
				continue
			}
			if _, _, ok := parsePaddingRange(sRange); !ok {
				return false
			}
		}
	}
	return true
}

func parsePaddingRange(sRange string) (int, int, bool) {
	sRangeMinMax := strings.Split(sRange, "-")
	if len(sRangeMinMax) != 2 {
		return 0, 0, false
	}
	minValue, err := strconv.Atoi(sRangeMinMax[0])
	if err != nil {
		return 0, 0, false
	}
	maxValue, err := strconv.Atoi(sRangeMinMax[1])
	if err != nil {
		return 0, 0, false
	}
	if minValue > maxValue {
		minValue, maxValue = maxValue, minValue
	}
	if minValue <= 0 || maxValue <= 0 || maxValue > MaxRecordPayloadSize {
		return 0, 0, false
	}
	return minValue, maxValue, true
}
