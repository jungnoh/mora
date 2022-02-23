package database

import (
	"sync"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/command"
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

func NewDatabase(config util.Config) (*Database, error) {
	db := Database{}
	db.lock = util.LockSet{
		Disk:   util.NewRWMutexMap(),
		Log:    util.NewRWMutexMap(),
		Memory: util.NewRWMutexMap(),
	}
	db.config = config
	db.Mem.Map = make(map[string]*memory.MemoryPage)
	db.Mem.Lock = &db.lock
	db.Mem.Config = &db.config
	db.Disk.Lock = &db.lock
	db.Disk.Config = &db.config

	walInstance, err := wal.NewWriteAheadLog(&db.config, &db.lock, &db.Disk)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialize WAL")
	}
	db.Wal = walInstance
	return &db, nil
}

func (d *Database) loadPage(set page.CandleSet, lock bool) (*page.Page, error) {
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
		return loadedPage, nil
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
	if err := d.Mem.Insert(loadedPage); err != nil {
		return &page.Page{}, errors.Wrapf(err, "loadBlock memory insert failed (key %s)", key)
	}
	finalPtr := d.Mem.Access(key)
	if finalPtr == nil {
		panic(errors.Errorf("loadBlock has inserted memory but is still null (key %s)", key))
	}
	return finalPtr, nil
}

func (d *Database) executeCommand(cmd command.CommandContent, txId uint64, factory wal.PersistRunner) error {
	fullCmd := command.NewCommand(txId, cmd)
	// TODO: Write to mem
	if err := factory.Write(fullCmd); err != nil {
		return err
	}
	if err := fullCmd.Content.Persist(&pageAccessor{db: d}); err != nil {
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
