package command

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/concurrency"
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
	AcquirePage(set page.CandleSet, exclusive bool) (func(), error)
	GetPage(set page.CandleSet, exclusive bool) (*page.Page, error)
}

type NeededLock struct {
	Lock      concurrency.ResourceName
	Exclusive bool
}

type CommandContent interface {
	common.SizableBinaryReadWriter
	TypeId() CommandType
	NeededLocks() []NeededLock
	Execute(accessor PageSetAccessor) (interface{}, error)
	String() string
}
