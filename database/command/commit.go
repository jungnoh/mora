package command

import (
	"io"

	"github.com/jungnoh/mora/page"
)

type CommitCommand struct {
}

func (e *CommitCommand) Read(size uint32, r io.Reader) error {
	return nil
}

func (e *CommitCommand) Write(w io.Writer) (err error) {
	return nil
}

func (e *CommitCommand) BinarySize() uint32 {
	return 0
}

func (e *CommitCommand) TypeId() CommandType {
	return CommitCommandType
}

func (e *CommitCommand) TargetSets() []page.CandleSet {
	return []page.CandleSet{}
}

func (e *CommitCommand) Persist(_ *map[string]*page.Page) error {
	return nil
}
