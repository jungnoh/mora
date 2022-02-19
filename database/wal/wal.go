package wal

import (
	"github.com/jungnoh/mora/database/disk"
	"github.com/jungnoh/mora/database/util"
)

type WriteAheadLog struct {
	config *util.Config
	lock   *util.LockSet
	disk   *disk.Disk

	counter   *WalCounter
	persister *WalPersister
	flusher   *WalFlusher
}

func NewWriteAheadLog(config *util.Config, lock *util.LockSet, disk *disk.Disk) (WriteAheadLog, error) {
	resolver := WalFileResolver{Config: config}
	counter := WalCounter{}
	if err := counter.Open(resolver.Counter()); err != nil {
		return WriteAheadLog{}, err
	}
	persister := WalPersister{
		Disk:         disk,
		FileResolver: &resolver,
		Counter:      &counter,
	}
	if err := persister.Setup(); err != nil {
		return WriteAheadLog{}, err
	}

	flusher := WalFlusher{
		Disk:         disk,
		FileResolver: &resolver,
	}

	return WriteAheadLog{
		config:    config,
		lock:      lock,
		disk:      disk,
		counter:   &counter,
		persister: &persister,
		flusher:   &flusher,
	}, nil
}
