package wal

import (
	"io"
	"os"
	"sync"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/wal/entry"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type WalWriteFile struct {
	fd        *os.File
	writeLock sync.Mutex
}

func NewWalWriteFile(fd *os.File) WalWriteFile {
	return WalWriteFile{
		fd: fd,
	}
}

type walWriteBuilder struct {
	committed bool
	txId      uint64
	file      *WalWriteFile
}

func (w *WalWriteFile) Open(file string) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	fd, err := os.OpenFile(file, os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return err
	}
	w.fd = fd
	return nil
}

func (w *WalWriteFile) Write(e entry.WalEntry) error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()

	if _, err := w.fd.Seek(0, io.SeekEnd); err != nil {
		return errors.Wrap(err, "failed to seek wal page")
	}
	if err := e.Write(w.fd); err != nil {
		return errors.Wrap(err, "failed to seek wal page")
	}
	return nil
}

func (w *WalWriteFile) Close() error {
	w.writeLock.Lock()
	defer w.writeLock.Unlock()
	if w.fd == nil {
		return nil
	}
	return w.fd.Close()
}

func (w *WalWriteFile) NewBuilder(txId uint64) WalWriter {
	return &walWriteBuilder{
		committed: false,
		txId:      txId,
		file:      w,
	}
}

func (w *walWriteBuilder) Insert(set page.CandleSet, candles []common.TimestampCandle) error {
	if w.committed {
		return errors.New("already committed")
	}
	newContent := entry.NewWalInsertContent(set, candles)
	newEntry := entry.NewWalEntry(w.txId, &newContent)
	return w.file.Write(newEntry)
}

func (w *walWriteBuilder) Commit() error {
	if w.committed {
		return errors.New("already committed")
	}
	w.committed = true
	newEntry := entry.NewWalEntry(w.txId, &entry.WalCommitContent{})
	return w.file.Write(newEntry)
}
