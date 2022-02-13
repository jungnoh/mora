package common

import "io"

type BinaryReadWriter interface {
	Read(size uint32, r io.Reader) error
	Write(w io.Writer) error
}
