package wal

import (
	"encoding/binary"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
)

type WalCounter struct {
	fd         *os.File
	counter    uint64
	accessLock sync.Mutex
}

func (w *WalCounter) Open(file string) error {
	w.accessLock.Lock()
	defer w.accessLock.Unlock()

	fd, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE, 0755)
	if err != nil {
		return err
	}
	w.fd = fd

	primaryCounter, err := w.readFile()
	if err == io.EOF {
		w.counter = 0
		writeErr := w.writeFile(0)
		if writeErr != nil {
			return writeErr
		}
	} else if err != nil {
		return err
	} else {
		w.counter = primaryCounter
	}
	return nil
}

func (w *WalCounter) Now() uint64 {
	w.accessLock.Lock()
	defer w.accessLock.Unlock()

	return w.counter
}

func (w *WalCounter) Next() (uint64, error) {
	w.accessLock.Lock()
	defer w.accessLock.Unlock()

	nextValue := w.counter + 1
	if err := w.writeFile(nextValue); err != nil {
		return 0, err
	}
	w.counter = nextValue
	return nextValue, nil
}

func (w *WalCounter) readFile() (uint64, error) {
	if _, err := w.fd.Seek(0, io.SeekStart); err != nil {
		return 0, err
	}
	binData := make([]byte, 8)
	n, err := w.fd.Read(binData)
	if err != nil {
		return 0, err
	}
	if n < 8 {
		return 0, io.EOF
	}
	return binary.LittleEndian.Uint64(binData), nil
}

func (w *WalCounter) writeFile(value uint64) error {
	if _, err := w.fd.Seek(0, io.SeekStart); err != nil {
		return errors.Wrap(err, "failed to write counter value")
	}
	if err := binary.Write(w.fd, binary.LittleEndian, value); err != nil {
		return errors.Wrap(err, "failed to write counter value")
	}
	return nil
}
