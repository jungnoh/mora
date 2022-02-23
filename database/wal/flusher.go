package wal

import (
	"fmt"
	"io"
	"os"

	"github.com/rs/zerolog/log"

	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/disk"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type flusherTransaction struct {
	TxId      uint64
	Committed bool
	Entries   []command.Command
}

func (f *flusherTransaction) AddEntry(e command.Command) {
	f.Entries = append(f.Entries, e)
}

func (f *flusherTransaction) NeededPages() []page.CandleSet {
	result := make(map[string]page.CandleSet)
	for _, entry := range f.Entries {
		sets := entry.Content.TargetSets()
		for _, set := range sets {
			result[set.UniqueKey()] = set
		}
	}
	v := make([]page.CandleSet, 0, len(result))
	for _, value := range result {
		v = append(v, value)
	}
	return v
}

type flusherAccessor struct {
	f *WalFlusher
}

func (a *flusherAccessor) Acquire(set page.CandleSet) (func(), error) {
	return func() {}, nil
}

func (a *flusherAccessor) Get(set page.CandleSet) (*page.Page, error) {
	return a.f.loadedPages[set.UniqueKey()], nil
}

type WalFlusher struct {
	FileResolver *WalFileResolver
	Disk         *disk.Disk

	loadedPagesLock util.MutexMap
	loadedPages     map[string]*page.Page
}

func NewWalFlusher(resolver *WalFileResolver, disk *disk.Disk) WalFlusher {
	return WalFlusher{
		FileResolver:    resolver,
		Disk:            disk,
		loadedPagesLock: util.NewMutexMap(),
		loadedPages:     make(map[string]*page.Page),
	}
}

func (w *WalFlusher) FlushWal(files []string) error {
	for _, file := range files {
		log.Debug().Str("file", file).Msg("flushing log")
		w.loadedPages = make(map[string]*page.Page)
		w.loadedPagesLock = util.NewMutexMap()
		if err := w.processFromDisk(file); err != nil {
			return errors.Wrapf(err, "failed to process log: %s", file)
		}
	}
	if err := w.flushToDisk(); err != nil {
		return errors.Wrap(err, "failed to write to disk")
	}
	for _, file := range files {
		if err := os.Remove(file); err != nil {
			return errors.Wrapf(err, "failed to delete log: %s", file)
		}
	}
	return nil
}
func (w *WalFlusher) processFromDisk(file string) error {
	readResult := make(map[uint64]*flusherTransaction)
	fd, err := os.Open(file)
	if err != nil {
		return errors.Wrap(err, "failed to open file")
	}
	reader := WalLogReader{fd: fd}
	for {
		e, err := reader.Read()
		if err == io.EOF {
			break
		}
		if _, ok := readResult[e.TxID]; !ok {
			readResult[e.TxID] = &flusherTransaction{
				TxId: e.TxID,
			}
		}
		if e.Type == command.CommitCommandType {
			readResult[e.TxID].Committed = true
			if err := w.flushToMemory(readResult[e.TxID]); err != nil {
				return err
			}
			delete(readResult, e.TxID)
		} else {
			readResult[e.TxID].AddEntry(e)
		}
	}

	for _, value := range readResult {
		if value.Committed {
			if err := w.flushToMemory(value); err != nil {
				return err
			}
		}
	}

	return nil
}

func (w *WalFlusher) flushToMemory(tx *flusherTransaction) error {
	neededPages := tx.NeededPages()
	fmt.Println(neededPages)
	for _, set := range neededPages {
		// Acquire lock
		pageKey := set.UniqueKey()
		lock := w.loadedPagesLock.Get(pageKey)
		lock.Lock()
		defer lock.Unlock()

		// Load into memory if needed
		if _, ok := w.loadedPages[pageKey]; !ok {
			loadedPage, err := w.Disk.Read(set)
			if err != nil {
				return errors.Wrapf(err, "failed to load page with key '%s' (tx=%d)", pageKey, tx.TxId)
			}
			if loadedPage.IsZero() {
				loadedPage = page.NewPage(set)
			}
			w.loadedPages[pageKey] = &loadedPage
		}

		// Update TxId
		if w.loadedPages[pageKey].Header.LastTxId < tx.TxId {
			w.loadedPages[pageKey].Header.LastTxId = tx.TxId
		}
	}

	for _, e := range tx.Entries {
		if err := e.Content.Persist(&flusherAccessor{f: w}); err != nil {
			return errors.Wrapf(err, "failed to persist (tx=%d)", tx.TxId)
		}
	}

	return nil
}

func (w *WalFlusher) flushToDisk() error {
	for key, page := range w.loadedPages {
		if err := w.Disk.Write(*page); err != nil {
			return errors.Wrapf(err, "failed to write page: key '%s'", key)
		}
	}
	return nil
}
