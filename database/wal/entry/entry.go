package entry

import (
	"encoding/binary"
	"io"

	"github.com/jungnoh/mora/common"
	"github.com/pkg/errors"
)

type WalEntryContent interface {
	common.SizableBinaryReadWriter
	TypeId() uint32
}

type WalEntry struct {
	TxID    uint64
	Type    uint32
	Content WalEntryContent
}

func NewWalEntry(txId uint64, content WalEntryContent) WalEntry {
	return WalEntry{
		TxID:    txId,
		Type:    content.TypeId(),
		Content: content,
	}
}

func (e *WalEntry) Read(_ uint32, r io.Reader) error {
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
	e.Type = binary.LittleEndian.Uint32(headerBytes[12:16])
	switch e.Type {
	case ENTRYID_COMMIT:
		e.Content = &WalCommitContent{}
	case ENTRYID_INSERT:
		e.Content = &WalCommitContent{}
	default:
		return errors.Errorf("unknown entry type %d", e.Type)
	}

	if err := e.Content.Read(entrySize, r); err != nil {
		return errors.Wrap(err, "failed to read entry content")
	}
	return nil
}

func (e *WalEntry) Write(w io.Writer) error {
	if e.Content == nil {
		return errors.New("entry content is nil")
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

func (e *WalEntry) BinarySize() uint32 {
	if e.Content == nil {
		return 16
	}
	return 16 + e.Content.BinarySize()
}
