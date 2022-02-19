package entry

import (
	"io"

	"github.com/jungnoh/mora/page"
)

type WalCommitContent struct {
}

func (e *WalCommitContent) Read(size uint32, r io.Reader) error {
	return nil
}

func (e *WalCommitContent) Write(w io.Writer) (err error) {
	return nil
}

func (e *WalCommitContent) BinarySize() uint32 {
	return 0
}

func (e *WalCommitContent) TypeId() EntryType {
	return ENTRYID_COMMIT
}

func (e *WalCommitContent) TargetSets() []page.CandleSet {
	return []page.CandleSet{}
}

func (e *WalCommitContent) Persist(_ *map[string]*page.Page) error {
	return nil
}
