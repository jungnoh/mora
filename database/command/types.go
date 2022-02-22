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

type CommandContent interface {
	common.SizableBinaryReadWriter
	TypeId() CommandType
	TargetSets() []page.CandleSet
	Persist(pages *map[string]*page.Page) error
}
