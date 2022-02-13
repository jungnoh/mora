package entry

import "io"

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

func (e *WalCommitContent) TypeId() uint32 {
	return ENTRYID_COMMIT
}
