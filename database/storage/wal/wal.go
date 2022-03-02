package wal

import (
	"sync"

	"github.com/jungnoh/mora/database/storage/disk"
	"github.com/jungnoh/mora/database/util"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type WriteAheadLog struct {
	config    *util.Config
	disk      *disk.Disk
	resolver  WalFileResolver
	counter   *WalCounter
	persister *WalPersister

	accessLock    sync.Mutex
	flusher       *WalFlusher
	flushChan     chan bool
	FlushDoneChan chan bool

	isFlushRunning bool
}

func NewWriteAheadLog(config *util.Config, disk *disk.Disk) (*WriteAheadLog, error) {
	resolver := WalFileResolver{Config: config}
	counter := WalCounter{}
	if err := counter.Open(resolver.Counter()); err != nil {
		return &WriteAheadLog{}, err
	}
	persister := WalPersister{
		Disk:         disk,
		FileResolver: &resolver,
		Counter:      &counter,
	}
	if err := persister.Setup(); err != nil {
		return &WriteAheadLog{}, err
	}

	flusher := NewWalFlusher(&resolver, disk)

	wal := WriteAheadLog{
		config:        config,
		disk:          disk,
		counter:       &counter,
		persister:     &persister,
		flusher:       &flusher,
		resolver:      resolver,
		flushChan:     make(chan bool),
		FlushDoneChan: make(chan bool),
	}
	go wal.listenToFlush()
	return &wal, nil
}

func (w *WriteAheadLog) Close() {
	w.persister.Close()
}

func (w *WriteAheadLog) Flush() {
	w.flushChan <- true
}

func (w *WriteAheadLog) Begin() (uint64, PersistRunner, error) {
	w.accessLock.Lock()
	defer w.accessLock.Unlock()

	txId, err := w.counter.Next()
	if err != nil {
		return 0, PersistRunner{}, err
	}
	builder, err := w.persister.StartBuilder()
	if err != nil {
		return 0, PersistRunner{}, errors.Wrapf(err, "failed to create tx builder (tx=%d)", txId)
	}
	return txId, builder, err
}

func (w *WriteAheadLog) listenToFlush() {
	// channel should close when WriteAheadLog is closed; no context is needed
	for range w.flushChan {
		err := w.execFlush()
		if err != nil {
			log.Warn().Err(err).Msg("Failed to flush write ahead log")
		} else {
			log.Debug().Msg("WAL flush complete")
			select {
			case w.FlushDoneChan <- true:
			default:
			}
		}
	}
}

func (w *WriteAheadLog) execFlush() error {
	w.accessLock.Lock()
	if w.isFlushRunning {
		w.accessLock.Unlock()
		return nil
	}
	w.isFlushRunning = true
	targetFiles, err := w.listFlushTargets()
	if err != nil {
		w.accessLock.Unlock()
		return errors.Wrap(err, "failed to list flush targets")
	}
	w.accessLock.Unlock()

	err = w.flusher.FlushWal(targetFiles)

	w.accessLock.Lock()
	w.isFlushRunning = false
	w.accessLock.Unlock()
	return err
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
