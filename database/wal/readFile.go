package wal

import (
	"io"
	"os"

	"github.com/jungnoh/mora/database/wal/entry"
)

type WalEntryMap map[uint64]*WalReadResult

type WalReadResult struct {
	Entries   []entry.WalEntry
	Committed bool
}

func (w *WalReadResult) AddEntry(e entry.WalEntry) {
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
		newEntry := entry.WalEntry{}
		err := newEntry.Read(0, w.fd)
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if _, ok := (*result)[newEntry.TxID]; !ok {
			(*result)[newEntry.TxID] = &WalReadResult{
				Entries: make([]entry.WalEntry, 0),
			}
		}
		(*result)[newEntry.TxID].AddEntry(newEntry)
		if _, ok := newEntry.Content.(*entry.WalCommitContent); ok {
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
		newEntry := entry.WalEntry{}
		err := newEntry.ReadOnlyHeader(0, w.fd)
		if err == io.EOF {
			break
		}
		if err != nil {
			return map[uint64]bool{}, err
		}
		if _, ok := result[newEntry.TxID]; !ok {
			result[newEntry.TxID] = false
		}
		result[newEntry.TxID] = result[newEntry.TxID] || (newEntry.Type == entry.ENTRYID_COMMIT)
	}
	return result, nil
}

func (w WalLogReader) SeekToStart() error {
	_, err := w.fd.Seek(0, io.SeekStart)
	return err
}

func (w WalLogReader) Read() (e entry.WalEntry, err error) {
	err = e.Read(0, w.fd)
	return
}
