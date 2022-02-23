package command

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/page"
)

type CommandType uint32

const (
	CommitCommandType CommandType = 1
	InsertCommandType CommandType = 2
)

type Command struct {
	TxID    uint64
	Type    CommandType
	Content CommandContent
}

type PageSetAccessor interface {
	Acquire(set page.CandleSet) (func(), error)
	Get(set page.CandleSet) (*page.Page, error)
}

type CommandContent interface {
	common.SizableBinaryReadWriter
	TypeId() CommandType
	TargetSets() []page.CandleSet
	Persist(accessor PageSetAccessor) error
	String() string
}
