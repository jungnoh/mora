package storage

import (
	"context"

	diskImpl "github.com/jungnoh/mora/database/storage/disk"
	memImpl "github.com/jungnoh/mora/database/storage/memory"
	walImpl "github.com/jungnoh/mora/database/storage/wal"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"

	"github.com/jungnoh/mora/database/util"
)

type Storage struct {
	config *util.Config
	disk   diskImpl.Disk
	memory memImpl.Memory
	wal    *walImpl.WriteAheadLog

	txLock   *util.RWMutexSet
	loadLock *util.MutexSet

	ctx           context.Context
	ctxCancel     context.CancelFunc
	diskLoadChan  chan diskLoadRequest
	diskStoreChan chan diskStoreRequest
}

func NewStorage(config *util.Config) *Storage {
	ctx, ctxCancel := context.WithCancel(context.Background())

	s := Storage{
		config:    config,
		txLock:    util.NewRWMutexSet("storageTx"),
		disk:      diskImpl.NewDisk(config),
		memory:    memImpl.Memory{},
		ctx:       ctx,
		ctxCancel: ctxCancel,
	}
	wal, err := walImpl.NewWriteAheadLog(config, &s.disk)
	if err != nil {
		panic(err)
	}
	s.wal = wal
	s.startTasks()
	return &s
}

func (s *Storage) startTasks() {
	go s.processDiskLoads()
	go s.processDiskStores()
}

func (s *Storage) Stop() {
	s.ctxCancel()
	// TODO: Kill(rollback) active accessors
}

func (s *Storage) Access() (StorageAccessor, error) {
	accessor := StorageAccessor{
		storage:  s,
		started:  false,
		finished: false,
		readers:  make(map[string]*memImpl.MemoryReader),
		writers:  make(map[string]*memImpl.MemoryWriter),
	}
	if err := accessor.start(); err != nil {
		return StorageAccessor{}, errors.Wrap(err, "failed to init StorageAccessor")
	}
	return accessor, nil
}

func (s *Storage) checkAndLoad(set page.CandleSet) error {
	key := set.UniqueKey()
	unlock := s.loadLock.Lock(key)
	defer unlock()

	exists := s.memory.HasPage(set)
	if exists {
		return nil
	}
	loaded, exists, err := s.diskLoad(set)
	if err != nil {
		return err
	}
	if exists {
		s.memory.ForceWrite(set, &loaded)
	}
	return nil
}

func (s *Storage) read(txId uint64, set page.CandleSet) (memImpl.MemoryReader, error) {
	err := s.checkAndLoad(set)
	if err != nil {
		return memImpl.MemoryReader{}, nil
	}
	reader, ok := s.memory.Read(txId, set)
	if !ok {
		return memImpl.MemoryReader{}, errors.New("could not load memory page")
	}
	return reader, nil
}

func (s *Storage) write(txId uint64, set page.CandleSet) (memImpl.MemoryWriter, error) {
	err := s.checkAndLoad(set)
	if err != nil {
		return memImpl.MemoryWriter{}, nil
	}
	writer := s.memory.StartWrite(txId, set)
	return writer, nil
}
