package storage

import (
	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/storage/memory"
	"github.com/jungnoh/mora/database/storage/wal"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type StorageAccessor struct {
	txId       uint64
	walFactory wal.PersistRunner
	storage    *Storage
	started    bool
	finished   bool

	readers map[string]*memory.MemoryReader
	writers map[string]*memory.MemoryWriter
}

func (s *StorageAccessor) start() error {
	txId, factory, err := s.storage.wal.Begin()
	if err != nil {
		return err
	}
	s.txId = txId
	s.walFactory = factory
	return nil
}

func (s *StorageAccessor) checkLock() {
	if s.started {
		panic(errors.New("trying to lock after start"))
	}
	if s.finished {
		panic(errors.New("trying to lock after close"))
	}
}

func (s *StorageAccessor) checkUse() {
	if !s.started {
		panic(errors.New("trying to use before start"))
	}
	if s.finished {
		panic(errors.New("trying to use after close"))
	}
}

func (s *StorageAccessor) AddRead(set page.CandleSet) error {
	s.checkLock()
	key := set.UniqueKey()
	if _, ok := s.writers[key]; ok {
		panic(errors.Errorf("trying to Slock '%s' after Xlock", key))
	}
	if _, ok := s.readers[key]; ok {
		return nil
	}
	reader, err := s.storage.read(s.txId, set)
	if err != nil {
		return err
	}
	s.readers[key] = &reader
	return nil
}

func (s *StorageAccessor) AddWrite(set page.CandleSet) error {
	s.checkLock()
	key := set.UniqueKey()
	if _, ok := s.readers[key]; ok {
		panic(errors.Errorf("trying to Xlock '%s' after Slock", key))
	}
	if _, ok := s.writers[key]; ok {
		return nil
	}
	writer, err := s.storage.write(s.txId, set)
	if err != nil {
		return err
	}
	s.writers[key] = &writer
	return nil
}

func (s *StorageAccessor) Start() {
	s.checkLock()
	s.started = true
}

func (s *StorageAccessor) Get(set page.CandleSet) (*page.Page, error) {
	s.checkUse()
	key := set.UniqueKey()
	if dd, ok := s.writers[key]; ok {
		return dd.WritableContent(), nil
	}
	if dd, ok := s.readers[key]; ok {
		return dd.Get(), nil
	}
	return nil, errors.Errorf("cannot find page '%s'", key)
}

func (s *StorageAccessor) Execute(cmd command.CommandContent) error {
	fullCmd := command.NewCommand(s.txId, cmd)
	if err := s.walFactory.Write(fullCmd); err != nil {
		return err
	}
	if err := fullCmd.Content.Persist(s); err != nil {
		return err
	}
	return nil
}

func (s *StorageAccessor) Commit() error {
	s.checkUse()
	defer s.walFactory.Close()
	if err := s.execCommit(); err != nil {
		return err
	}
	for _, reader := range s.readers {
		reader.Done()
	}
	for _, writer := range s.writers {
		writer.Commit()
	}
	s.finished = true
	return nil
}

func (s *StorageAccessor) execCommit() error {
	if err := s.walFactory.Write(command.NewCommand(s.txId, &command.CommitCommand{})); err != nil {
		return errors.Wrap(err, "failed to log commit")
	}
	return nil
}

func (s *StorageAccessor) Rollback() {
	s.checkUse()
	defer s.walFactory.Close()
	for _, reader := range s.readers {
		reader.Done()
	}
	for _, writer := range s.writers {
		writer.Rollback()
	}
	s.finished = true
}

// Stub methods to implement database.pageAccessor
func (s *StorageAccessor) Acquire(set page.CandleSet) (func(), error) {
	return func() {}, nil
}
func (s *StorageAccessor) Free() {
}
