package command

import (
	"encoding/binary"
	"io"

	"github.com/pkg/errors"
)

func NewCommand(txId uint64, content CommandContent) Command {
	return Command{
		TxID:    txId,
		Type:    content.TypeId(),
		Content: content,
	}
}

func (e *Command) Read(_ uint32, r io.Reader) error {
	headerBytes := make([]byte, 16)
	n, err := r.Read(headerBytes)
	if n < 16 {
		return io.EOF
	}
	if err != nil {
		return err
	}

	entrySize := binary.LittleEndian.Uint32(headerBytes[0:4])
	e.TxID = binary.LittleEndian.Uint64(headerBytes[4:12])
	e.Type = CommandType(binary.LittleEndian.Uint32(headerBytes[12:16]))
	switch e.Type {
	case CommitCommandType:
		e.Content = &CommitCommand{}
	case InsertCommandType:
		e.Content = &CommitCommand{}
	default:
		return errors.Errorf("unknown entry type %d", e.Type)
	}

	if err := e.Content.Read(entrySize, r); err != nil {
		return errors.Wrap(err, "failed to read entry content")
	}
	return nil
}

func (e *Command) ReadHeader(_ uint32, r io.ReadSeeker) error {
	headerBytes := make([]byte, 16)
	n, err := r.Read(headerBytes)
	if n < 16 {
		return io.EOF
	}
	if err != nil {
		return err
	}

	entrySize := binary.LittleEndian.Uint32(headerBytes[0:4])
	e.TxID = binary.LittleEndian.Uint64(headerBytes[4:12])
	e.Type = CommandType(binary.LittleEndian.Uint32(headerBytes[12:16]))

	if _, err := r.Seek(int64(entrySize), io.SeekCurrent); err != nil {
		return errors.Wrap(err, "failed to seek")
	}
	return nil
}

func (e *Command) Write(w io.Writer) error {
	if e.Content == nil {
		return errors.New("command content is nil")
	}
	bodySize := e.Content.BinarySize()
	if err := binary.Write(w, binary.LittleEndian, bodySize); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.TxID); err != nil {
		return err
	}
	if err := binary.Write(w, binary.LittleEndian, e.Type); err != nil {
		return err
	}
	if err := e.Content.Write(w); err != nil {
		return err
	}
	return nil
}

func (e *Command) BinarySize() uint32 {
	if e.Content == nil {
		return 16
	}
	return 16 + e.Content.BinarySize()
}
