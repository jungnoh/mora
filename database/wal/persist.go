package wal

import (
	"sync"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/disk"
	"github.com/jungnoh/mora/page"
)

// TODO: Move to config
const MAX_COMMITTED_PAGES int = 256

type WalPersister struct {
	Disk         *disk.Disk
	FileResolver *WalFileResolver
	Counter      *WalCounter

	lock         sync.Mutex
	currentFile  WalWriteFile
	writtenCount int
	flushChan    chan<- bool
}

func (w *WalPersister) addWrittenCount() error {
	w.writtenCount++
	if w.writtenCount < MAX_COMMITTED_PAGES {
		return nil
	}

	fd, err := w.FileResolver.NewFile(w.Counter.Now())
	if err != nil {
		return err
	}
	w.currentFile.Close()

	w.currentFile = NewWalWriteFile(fd)
	w.writtenCount = 0
	return nil
}

type walPersisterBuilder struct {
	fileBuilder *walWriteBuilder
	persister   *WalPersister
}

func (w *walPersisterBuilder) Insert(set page.CandleSet, candles []common.TimestampCandle) error {
	w.persister.lock.Lock()
	defer w.persister.lock.Unlock()
	return w.fileBuilder.Insert(set, candles)
}

func (w *walPersisterBuilder) Commit() error {
	w.persister.lock.Lock()
	defer w.persister.lock.Unlock()
	if err := w.fileBuilder.Commit(); err != nil {
		return err
	}
	if err := w.persister.addWrittenCount(); err != nil {
		return err
	}
	go func() {
		w.persister.flushChan <- true
	}()
	return nil
}
