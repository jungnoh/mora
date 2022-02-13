package database

import (
	"sync"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/disk"
	"github.com/jungnoh/mora/database/memory"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/database/wal"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type Database struct {
	config util.Config
	lock   util.LockSet
	Mem    memory.Memory
	Disk   disk.Disk
	Wal    wal.WriteAheadLog
}

func NewDatabase(config util.Config) *Database {
	db := Database{}
	db.config = config
	db.Mem.Lock = &db.lock
	db.Mem.Config = &db.config
	db.Disk.Lock = &db.lock
	db.Disk.Config = &db.config
	return &db
}

func (d *Database) loadPage(set page.CandleSet, lock bool) (page.Page, error) {
	key := set.UniqueKey()
	var pageLock *sync.RWMutex = nil
	if lock {
		pageLock := d.lock.Memory.Get(key)
		pageLock.RLock()
	}

	// In cache -> load and return
	if d.Mem.Exists(key) {
		loadedPage := d.Mem.Access(key)
		if lock {
			pageLock.RUnlock()
		}
		return *loadedPage, nil
	}

	if lock {
		pageLock.RUnlock()
		pageLock.Lock()
		defer pageLock.Unlock()
	}
	loadedPage, err := d.Disk.Read(set)
	if err != nil {
		return page.Page{}, errors.Wrap(err, "loadBlock disk read failed")
	}
	if err := d.Mem.Insert(loadedPage); err != nil {
		return page.Page{}, errors.Wrap(err, "loadBlock memory insert failed")
	}

	return loadedPage, nil
}

func (d *Database) writePage(set page.CandleSet, candles []common.Candle) error {
	key := set.UniqueKey()
	pageLock := d.lock.Memory.Get(key)

	pageLock.Lock()
	defer pageLock.Unlock()
	// d.lock.WAL.Lock()
	// defer d.lock.WAL.Unlock()

	// txID := d.Wal.NextTxID()
	_, err := d.loadPage(set, false)
	if err != nil {
		return errors.Wrap(err, "writePage failed: loading page")
	}
	memPage := d.Mem.AccessMemoryPage(set.UniqueKey())
	if memPage == nil {
		return errors.New("writePage failed: memPage is nil")
	}

	// if err := d.Wal.Write(txID, set, candles); err != nil {
	// 	return errors.Wrap(err, "writePage failed: write to WAL")
	// }
	if err := d.Mem.Write(set, candles); err != nil {
		return errors.Wrap(err, "writePage failed: write to memory")
	}
	return nil
}

func (d *Database) Write(set page.CandleSetWithoutYear, candles []common.Candle) error {
	years := common.SplitCandlesByYear(candles)
	wg := sync.WaitGroup{}
	wg.Add(len(years))

	errList := make([]error, 0)
	for key := range years {
		year := key
		go func() {
			err := d.writePage(page.CandleSet{
				CandleSetWithoutYear: set,
				Year:                 year,
			}, years[year])
			if err != nil {
				errList = append(errList, err)
			}
			wg.Done()
		}()
	}
	wg.Wait()

	if len(errList) > 0 {
		return errList[0]
	}
	return nil
}
