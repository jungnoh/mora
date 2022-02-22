package wal

import (
	"io"
	"os"

	"github.com/jungnoh/mora/database/command"
)

type WalEntryMap map[uint64]*WalReadResult

type WalReadResult struct {
	Entries   []command.Command
	Committed bool
}

func (w *WalReadResult) AddEntry(e command.Command) {
	w.Entries = append(w.Entries, e)
}

func (w *WalReadResult) SetAsCommitted() {
	w.Committed = true
}

type WalLogReader struct {
	fd *os.File
}

func (w WalLogReader) ReadAll(result *WalEntryMap) error {
	if _, err := w.fd.Seek(0, io.SeekStart); err != nil {
		return err
	}
	for {
		newEntry := command.Command{}
		err := newEntry.Read(0, w.fd)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, ok := (*result)[newEntry.TxID]; !ok {
			(*result)[newEntry.TxID] = &WalReadResult{
				Entries: make([]command.Command, 0),
			}
		}
		(*result)[newEntry.TxID].AddEntry(newEntry)
		if newEntry.Type == command.CommitCommandType {
			(*result)[newEntry.TxID].SetAsCommitted()
		}
	}
	return nil
}

func (w WalLogReader) ListCommittedAll() (map[uint64]bool, error) {
	if _, err := w.fd.Seek(0, io.SeekStart); err != nil {
		return map[uint64]bool{}, err
	}
	result := make(map[uint64]bool)
	for {
		newEntry := command.Command{}
		err := newEntry.ReadHeader(0, w.fd)
		if err == io.EOF {
			break
		}
		if err != nil {
			return map[uint64]bool{}, err
		}
		if _, ok := result[newEntry.TxID]; !ok {
			result[newEntry.TxID] = false
		}
		result[newEntry.TxID] = result[newEntry.TxID] || (newEntry.Type == command.CommitCommandType)
	}
	return result, nil
}

func (w WalLogReader) SeekToStart() error {
	_, err := w.fd.Seek(0, io.SeekStart)
	return err
}

func (w WalLogReader) Read() (e command.Command, err error) {
	err = e.Read(0, w.fd)
	return
}
