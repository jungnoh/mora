package storage

import (
	"context"
	"errors"

	diskImpl "github.com/jungnoh/mora/database/storage/disk"
	memImpl "github.com/jungnoh/mora/database/storage/memory"
	"github.com/jungnoh/mora/page"

	"github.com/jungnoh/mora/database/util"
)

type Storage struct {
	config *util.Config
	disk   diskImpl.Disk
	memory memImpl.Memory

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

func (s *Storage) Access(txId uint64) StorageAccessor {
	return StorageAccessor{
		txId:     txId,
		storage:  s,
		started:  false,
		finished: false,
		readers:  make(map[string]*memImpl.MemoryReader),
		writers:  make(map[string]*memImpl.MemoryWriter),
	}
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
