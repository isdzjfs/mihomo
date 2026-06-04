package trie

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

const (
	maxDomainSetUint64Entries = 8 * 1024 * 1024
	maxDomainSetLabelsLength  = 64 * 1024 * 1024
)

func (ss *DomainSet) WriteBin(w io.Writer) (err error) {
	// version
	_, err = w.Write([]byte{1})
	if err != nil {
		return err
	}

	// leaves
	err = binary.Write(w, binary.BigEndian, int64(len(ss.leaves)))
	if err != nil {
		return err
	}
	for _, d := range ss.leaves {
		err = binary.Write(w, binary.BigEndian, d)
		if err != nil {
			return err
		}
	}

	// labelBitmap
	err = binary.Write(w, binary.BigEndian, int64(len(ss.labelBitmap)))
	if err != nil {
		return err
	}
	for _, d := range ss.labelBitmap {
		err = binary.Write(w, binary.BigEndian, d)
		if err != nil {
			return err
		}
	}

	// labels
	err = binary.Write(w, binary.BigEndian, int64(len(ss.labels)))
	if err != nil {
		return err
	}
	_, err = w.Write(ss.labels)
	if err != nil {
		return err
	}

	return nil
}

func ReadDomainSetBin(r io.Reader) (ds *DomainSet, err error) {
	// version
	version := make([]byte, 1)
	_, err = io.ReadFull(r, version)
	if err != nil {
		return nil, err
	}
	if version[0] != 1 {
		return nil, errors.New("version is invalid")
	}

	ds = &DomainSet{}
	var length int64

	// leaves
	err = binary.Read(r, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}
	if length < 1 {
		return nil, errors.New("length is invalid")
	}
	if length > maxDomainSetUint64Entries {
		return nil, fmt.Errorf("leaves length %d exceeds maximum %d", length, maxDomainSetUint64Entries)
	}
	ds.leaves = make([]uint64, length)
	for i := int64(0); i < length; i++ {
		err = binary.Read(r, binary.BigEndian, &ds.leaves[i])
		if err != nil {
			return nil, err
		}
	}

	// labelBitmap
	err = binary.Read(r, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}
	if length < 1 {
		return nil, errors.New("length is invalid")
	}
	if length > maxDomainSetUint64Entries {
		return nil, fmt.Errorf("label bitmap length %d exceeds maximum %d", length, maxDomainSetUint64Entries)
	}
	ds.labelBitmap = make([]uint64, length)
	for i := int64(0); i < length; i++ {
		err = binary.Read(r, binary.BigEndian, &ds.labelBitmap[i])
		if err != nil {
			return nil, err
		}
	}

	// labels
	err = binary.Read(r, binary.BigEndian, &length)
	if err != nil {
		return nil, err
	}
	if length < 1 {
		return nil, errors.New("length is invalid")
	}
	if length > maxDomainSetLabelsLength {
		return nil, fmt.Errorf("labels length %d exceeds maximum %d", length, maxDomainSetLabelsLength)
	}
	ds.labels = make([]byte, length)
	_, err = io.ReadFull(r, ds.labels)
	if err != nil {
		return nil, err
	}

	ds.init()
	return ds, nil
}
