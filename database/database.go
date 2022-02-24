package database

import (
	"context"
	"fmt"
	"sync"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/disk"
	"github.com/jungnoh/mora/database/memory"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/database/wal"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type Database struct {
	config          util.Config
	lock            util.LockSet
	Mem             memory.Memory
	Disk            disk.Disk
	Wal             wal.WriteAheadLog
	evcitedNotiChan chan *page.Page
	ctx             context.Context
	ctxCancel       context.CancelFunc
}

func NewDatabase(config util.Config) (*Database, error) {
	db := Database{}
	db.ctx, db.ctxCancel = context.WithCancel(context.Background())
	db.lock = util.NewLockSet()
	db.config = config
	db.Disk.Lock = &db.lock
	db.Disk.Config = &db.config
	db.evcitedNotiChan = make(chan *page.Page)

	mem, err := memory.NewMemory(config.MaxCleanBlocks, db.evcitedNotiChan)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize memory")
	}
	db.Mem = *mem
	db.Mem.Lock = &db.lock
	db.Mem.Config = &db.config

	walInstance, err := wal.NewWriteAheadLog(&db.config, &db.lock, &db.Disk)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize WAL")
	}
	db.Wal = walInstance

	go db.evict()
	return &db, nil
}

func (d *Database) evict() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case pg := <-d.evcitedNotiChan:
			err := d.execEvict(pg)
			if err != nil {
				log.Panic().Err(err).Msg("Eviction failed!")
			}
		}
	}
}

func (d *Database) execEvict(pg *page.Page) error {
	fmt.Println(*pg)
	return nil
}

func (d *Database) loadPage(set page.CandleSet, lock bool) (*page.Page, error) {
	key := set.UniqueKey()
	var pageLock *sync.RWMutex = nil
	if lock {
		pageLock := d.lock.Memory.Get(key)
		pageLock.RLock()
	}

	// In cache -> load and return
	pg, ok := d.Mem.GetPage(key)
	if ok {
		return pg, nil
	}

	if lock {
		pageLock.RUnlock()
		pageLock.Lock()
		defer pageLock.Unlock()
	}
	loadedPage, err := d.Disk.Read(set)
	if err != nil {
		return &page.Page{}, errors.Wrapf(err, "loadBlock disk read failed (key %s)", key)
	}
	if loadedPage.IsZero() {
		loadedPage = page.NewPage(set)
	}
	d.Mem.Insert(&loadedPage)
	return &loadedPage, nil
}

func (d *Database) executeCommand(cmd command.CommandContent, txId uint64, factory wal.PersistRunner) error {
	fullCmd := command.NewCommand(txId, cmd)
	if err := factory.Write(fullCmd); err != nil {
		return err
	}
	if err := fullCmd.Content.Persist(&pageAccessor{db: d, txId: txId}); err != nil {
		return err
	}
	return nil
}

func (d *Database) Execute(commands []command.CommandContent) error {
	txId, factory, err := d.Wal.Begin()
	if err != nil {
		return errors.Wrap(err, "exec: wal tx start failed")
	}
	defer factory.Close()
	for _, cmd := range commands {
		if err := d.executeCommand(cmd, txId, factory); err != nil {
			return errors.Wrapf(err, "exec cmd failed: %s", cmd.String())
		}
	}
	if err := factory.Write(command.NewCommand(txId, &command.CommitCommand{})); err != nil {
		return errors.Wrap(err, "failed to log commit")
	}
	return factory.Close()
}

// High level commands
func (d *Database) Write(set page.CandleSetWithoutYear, candles common.CandleList) error {
	commands := CommandContentFactory{}.InsertToSet(set, candles)
	return d.Execute(commands)
}
