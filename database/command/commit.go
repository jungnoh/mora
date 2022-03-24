package command

import (
	"io"
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

func (e *CommitCommand) Plan() CommandPlan {
	return CommandPlan{
		NeededLocks: []NeededLock{},
	}
}

func (e *CommitCommand) Execute(_ PageSetAccessor) (interface{}, error) {
	return struct{}{}, nil
}

func (e *CommitCommand) String() string {
	return "COMMIT"
}
