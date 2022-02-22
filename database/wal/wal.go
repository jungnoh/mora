package wal

import (
	"github.com/jungnoh/mora/database/disk"
	"github.com/jungnoh/mora/database/util"
	"github.com/pkg/errors"
)

type WriteAheadLog struct {
	config *util.Config
	lock   *util.LockSet
	disk   *disk.Disk

	resolver  WalFileResolver
	counter   *WalCounter
	persister *WalPersister
	flusher   *WalFlusher
	flushChan chan bool
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

	wal := WriteAheadLog{
		config:    config,
		lock:      lock,
		disk:      disk,
		counter:   &counter,
		persister: &persister,
		flusher:   &flusher,
		resolver:  resolver,
	}
	go wal.listenToFlush()
	return wal, nil
}

func (w *WriteAheadLog) Close() {
	w.persister.Close()
}

func (w *WriteAheadLog) Flush() {
	w.flushChan <- true
}

func (w *WriteAheadLog) Begin() (uint64, walPersisterBuilder, error) {
	w.lock.WAL.Lock()
	defer w.lock.WAL.Unlock()

	txId, err := w.counter.Next()
	if err != nil {
		return 0, walPersisterBuilder{}, err
	}
	builder, err := w.persister.StartBuilder()
	if err != nil {
		return 0, walPersisterBuilder{}, errors.Wrapf(err, "failed to create tx builder (tx=%d)", txId)
	}
	return txId, builder, err
}

func (w *WriteAheadLog) listenToFlush() {
	// channel should close when WriteAheadLog is closed; no context is needed
	for range w.flushChan {
		w.execFlush()
	}
}

func (w *WriteAheadLog) execFlush() error {
	w.lock.WAL.Lock()
	defer w.lock.WAL.Unlock()

	targetFiles, err := w.listFlushTargets()
	if err != nil {
		return errors.Wrap(err, "failed to list flush targets")
	}
	return w.flusher.FlushWal(targetFiles)
}

func (w *WriteAheadLog) listFlushTargets() ([]string, error) {
	files, err := w.resolver.AllFiles()
	if err != nil {
		return []string{}, err
	}

	targetFiles := make([]string, 0, len(files))
	w.persister.changeLogLock.Lock()
	defer w.persister.changeLogLock.Unlock()
	for _, file := range files {
		if file != w.persister.currentLog.filename {
			targetFiles = append(targetFiles, w.resolver.FullPath(file))
		}
	}

	return targetFiles, nil
}
