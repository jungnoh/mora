package wal

import (
	"github.com/jungnoh/mora/database/disk"
	"github.com/jungnoh/mora/database/util"
)

type WalPersister struct {
	Lock *util.LockSet
	Disk *disk.Disk
}
