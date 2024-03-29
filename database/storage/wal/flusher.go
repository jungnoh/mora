package wal

import (
	"io"
	"os"

	"github.com/rs/zerolog/log"

	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/storage/disk"
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

type flusherAccessor struct {
	f    *WalFlusher
	txId uint64
}

func (a *flusherAccessor) AcquirePage(set page.CandleSet, exclusive bool) (func(), error) {
	pageKey := set.UniqueKey()
	if _, ok := a.f.loadedPages[pageKey]; !ok {
		lock := a.f.loadedPagesLock.Get(pageKey)
		lock.Lock()
		defer lock.Unlock()

		loadedPage, err := a.f.Disk.Read(set)
		if err != nil {
			return func() {}, errors.Wrapf(err, "failed to load page with key '%s' (tx=%d)", pageKey, a.txId)
		}
		if loadedPage.IsZero() {
			loadedPage = page.NewPage(set)
		}
		a.f.loadedPages[pageKey] = &loadedPage
	}
	if a.f.loadedPages[pageKey].Header.LastTxId < a.txId {
		a.f.loadedPages[pageKey].Header.LastTxId = a.txId
	}
	return func() {}, nil
}

func (a *flusherAccessor) GetPage(set page.CandleSet, exclusive bool) (*page.Page, error) {
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
		log.Debug().Str("file", file).Msg("Flushing WAL log")
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
			log.Debug().Uint64("tx", e.TxID).Msg("Committing log")
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
	for _, entry := range tx.Entries {
		// TODO: Skip if possible
		if _, err := entry.Content.Execute(&flusherAccessor{f: w, txId: tx.TxId}); err != nil {
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
