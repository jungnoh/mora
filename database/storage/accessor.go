package storage

import (
	"sort"

	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/storage/memory"
	"github.com/jungnoh/mora/database/storage/wal"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type accessorNeededPage struct {
	set       page.CandleSet
	exclusive bool
}

type StorageAccessor struct {
	txId       uint64
	walFactory wal.PersistRunner
	storage    *Storage
	started    bool
	finished   bool

	todo    map[string]accessorNeededPage
	readers map[string]*memory.MemoryReader
	writers map[string]*memory.MemoryWriter
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

func (s *StorageAccessor) AddRead(set page.CandleSet) {
	s.checkLock()
	key := set.UniqueKey()
	s.todo[key] = accessorNeededPage{
		set:       set,
		exclusive: true,
	}
}

func (s *StorageAccessor) AddWrite(set page.CandleSet) {
	s.checkLock()
	key := set.UniqueKey()
	s.todo[key] = accessorNeededPage{
		set:       set,
		exclusive: true,
	}
}

func (s *StorageAccessor) Start() error {
	s.checkLock()

	txId, factory, err := s.storage.wal.Begin()
	if err != nil {
		return errors.Wrap(err, "failed to start transaction")
	}
	s.txId = txId
	s.walFactory = factory
	log.Debug().Uint64("id", s.txId).Msg("Tx START")

	keys := make([]string, len(s.todo))
	for key := range s.todo {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		if s.todo[key].exclusive {
			writer, err := s.storage.write(s.txId, s.todo[key].set)
			if err != nil {
				return errors.Wrapf(err, "failed to open write for set '%s'", key)
			}
			s.writers[key] = &writer
		} else {
			reader, err := s.storage.read(s.txId, s.todo[key].set)
			if err != nil {
				return errors.Wrapf(err, "failed to open read for set '%s'", key)
			}
			s.readers[key] = &reader
		}
	}

	s.started = true
	return nil
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
	log.Debug().Uint64("id", s.txId).Msg("Tx COMMIT")
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
	log.Debug().Uint64("id", s.txId).Msg("Tx ROLLBACK")
	defer s.walFactory.Close()
	for _, reader := range s.readers {
		reader.Done()
	}
	for _, writer := range s.writers {
		writer.Rollback()
	}
	s.finished = true
}

func (s *StorageAccessor) RollbackIfActive() {
	if !s.started || s.finished {
		return
	}
	s.Rollback()
}

// Stub methods to implement database.pageAccessor
func (s *StorageAccessor) Acquire(set page.CandleSet) (func(), error) {
	return func() {}, nil
}
func (s *StorageAccessor) Free() {
}
