package wal

import (
	"io"
	"os"
	"sync"

	"github.com/jungnoh/mora/database/command"
	"github.com/pkg/errors"
)

type WalWriteFile struct {
	fd       *os.File
	filename string
	fileLock sync.Mutex
}

func NewWalWriteFile(fd *os.File, filename string) WalWriteFile {
	return WalWriteFile{
		fd:       fd,
		filename: filename,
	}
}

type walWriteBuilder struct {
	committed bool
	txId      uint64
	file      *WalWriteFile
}

func (w *WalWriteFile) Open(file string) error {
	w.fileLock.Lock()
	defer w.fileLock.Unlock()

	fd, err := os.OpenFile(file, os.O_RDWR|os.O_APPEND, 0755)
	if err != nil {
		return err
	}
	w.fd = fd
	return nil
}

func (w *WalWriteFile) Write(e command.Command) error {
	w.fileLock.Lock()
	defer w.fileLock.Unlock()

	if _, err := w.fd.Seek(0, io.SeekEnd); err != nil {
		return errors.Wrap(err, "failed to seek wal page")
	}
	if err := e.Write(w.fd); err != nil {
		return errors.Wrap(err, "failed to seek wal page")
	}
	return nil
}

func (w *WalWriteFile) Close() error {
	w.fileLock.Lock()
	defer w.fileLock.Unlock()
	if w.fd == nil {
		return nil
	}
	return w.fd.Close()
}
