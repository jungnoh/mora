package wal

import (
	"context"
	"sync"

	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/storage/disk"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

// TODO: Move to config
const MAX_COMMITTED_PAGES int = 256

type WalPersister struct {
	Disk         *disk.Disk
	FileResolver *WalFileResolver
	Counter      *WalCounter

	currentLog     WalWriteFile
	currentLogLock sync.RWMutex
	changeLogLock  sync.Mutex
	writtenCount   int
	rotateChan     chan string
	flushChan      chan bool
	ctxCancel      context.CancelFunc
}

func (w *WalPersister) Setup() error {
	if err := w.RotateFile(); err != nil {
		return errors.Wrap(err, "WAL rotation failed!")
	}
	ctx, cancel := context.WithCancel(context.Background())
	w.ctxCancel = cancel
	w.flushChan = make(chan bool)
	go w.watchRotateChan(ctx)
	return nil
}

func (w *WalPersister) Close() {
	w.currentLogLock.Lock()
	defer w.currentLogLock.Unlock()

	w.currentLog.Close()
	close(w.flushChan)
	w.ctxCancel()
}

func (w *WalPersister) StartBuilder() (PersistRunner, error) {
	w.currentLogLock.RLock()
	return PersistRunner{
		persister: w,
	}, nil
}

func (w *WalPersister) watchRotateChan(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case file := <-w.rotateChan:
			if w.currentLog.filename == file {
				continue
			}
			err := w.RotateFile()
			if err != nil {
				log.Panic().Err(err).Msg("WAL rotation failed!")
			}
		}
	}
}

func (w *WalPersister) addWrittenCount() {
	w.writtenCount++
	if w.writtenCount >= MAX_COMMITTED_PAGES {
		w.rotateChan <- w.currentLog.filename
	}
}

func (w *WalPersister) RotateFile() error {
	w.currentLogLock.Lock()
	defer w.currentLogLock.Unlock()
	w.changeLogLock.Lock()
	defer w.changeLogLock.Unlock()

	fd, filename, err := w.FileResolver.NewFile(w.Counter.Now())
	if err != nil {
		return err
	}
	w.currentLog.Close()

	w.currentLog = NewWalWriteFile(fd, filename)
	w.writtenCount = 0

	select {
	case w.flushChan <- true:
		break
	default:
		break
	}
	return nil
}

type PersistRunner struct {
	persister *WalPersister
	closed    bool
}

func (w *PersistRunner) Write(e command.Command) error {
	return w.persister.currentLog.Write(e)
}

func (w *PersistRunner) Close() error {
	if w.closed {
		return nil
	}
	w.closed = true
	w.persister.currentLogLock.RUnlock()
	w.persister.addWrittenCount()
	return nil
}
