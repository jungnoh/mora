package wal

import "github.com/jungnoh/mora/database/util"

type WriteAheadLog struct {
	Lock *util.LockSet
	TxId uint64
}
