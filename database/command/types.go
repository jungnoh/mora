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

type CommandPlan struct {
	NeededLocks NeededLockSlice
}

type PageSetAccessor interface {
	AcquirePage(set page.CandleSet, exclusive bool) (func(), error)
	GetPage(set page.CandleSet, exclusive bool) (*page.Page, error)
}

type NeededLock struct {
	Lock      concurrency.ResourceName
	Exclusive bool
}

type NeededLockSlice []NeededLock

func (n NeededLockSlice) Len() int {
	return len(n)
}
func (n NeededLockSlice) Swap(i, j int) {
	n[i], n[j] = n[j], n[i]
}
func (n NeededLockSlice) Less(i, j int) bool {
	return n[i].Lock.Hash() < n[j].Lock.Hash()
}

type CommandContent interface {
	common.SizableBinaryReadWriter
	TypeId() CommandType
	Plan() CommandPlan
	Execute(accessor PageSetAccessor) (interface{}, error)
	String() string
}
