package common

import (
	"encoding/binary"
	"io"
	"math"
	"strings"

	"github.com/pkg/errors"
)

type BinaryReadWriter interface {
	Read(size uint32, r io.Reader) error
	Write(w io.Writer) error
}

type SizableBinaryReadWriter interface {
	BinaryReadWriter
	BinarySize() uint32
}

func Float64frombytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func WriteNullPaddedString(length int, str string, w io.Writer) error {
	encodedStr := []byte(str)
	if len(encodedStr) > length {
		return errors.Errorf("string is too long (maximum %d, got %d)", length, len(encodedStr))
	}
	if _, err := w.Write(encodedStr); err != nil {
		return err
	}
	if _, err := w.Write(make([]byte, length-len(encodedStr))); err != nil {
		return err
	}
	return nil
}

func ReadNullPaddedString(data []byte) string {
	return strings.TrimRight(string(data), "\u0000")
}
