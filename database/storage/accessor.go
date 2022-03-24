package storage

import (
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

	readers map[string]*memory.MemoryReader
	writers map[string]*memory.MemoryWriter
}

func (s *StorageAccessor) checkUse() {
	if !s.started {
		panic(errors.New("trying to use before start"))
	}
	if s.finished {
		panic(errors.New("trying to use after close"))
	}
}

func (s *StorageAccessor) addRead(set page.CandleSet) error {
	s.checkUse()
	key := set.UniqueKey()

	if _, ok := s.writers[key]; ok {
		return nil
	}
	if _, ok := s.readers[key]; ok {
		return nil
	}
	reader, err := s.storage.read(s.txId, set)
	if err != nil {
		return errors.Wrapf(err, "failed to open write for set '%s'", key)
	}
	s.readers[key] = &reader
	return nil
}

func (s *StorageAccessor) addWrite(set page.CandleSet) error {
	s.checkUse()
	key := set.UniqueKey()

	if _, ok := s.writers[key]; ok {
		return nil
	}
	writer, err := s.storage.write(s.txId, set)
	if err != nil {
		return errors.Wrapf(err, "failed to open read for set '%s'", key)
	}
	s.writers[key] = &writer
	return nil
}

func (s *StorageAccessor) Start() (txId uint64, err error) {
	if s.started || s.finished {
		panic(errors.New("already used"))
	}
	txId, factory, err := s.storage.wal.Begin()
	if err != nil {
		return 0, errors.Wrap(err, "failed to start transaction")
	}
	s.txId = txId
	s.walFactory = factory
	log.Debug().Uint64("id", s.txId).Msg("Tx START")

	s.started = true
	return s.txId, nil
}

func (s *StorageAccessor) Execute(cmd command.CommandContent) (interface{}, error) {
	fullCmd := command.NewCommand(s.txId, cmd)
	if err := s.walFactory.Write(fullCmd); err != nil {
		return struct{}{}, err
	}
	return fullCmd.Content.Execute(s)
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

// Methods to implement database.pageAccessor
func (s *StorageAccessor) GetPage(set page.CandleSet, exclusive bool) (*page.Page, error) {
	s.checkUse()
	key := set.UniqueKey()

	if dd, ok := s.writers[key]; ok {
		return dd.WritableContent(), nil
	}
	if dd, ok := s.readers[key]; ok && !exclusive {
		return dd.Get(), nil
	}
	if exclusive {
		err := s.addWrite(set)
		if err != nil {
			return nil, errors.Wrap(err, "failed to add write")
		}
		return s.writers[key].WritableContent(), nil
	}
	err := s.addRead(set)
	if err != nil {
		return nil, errors.Wrap(err, "failed to add read")
	}
	return s.readers[key].Get(), nil
}

func (s *StorageAccessor) AcquirePage(set page.CandleSet, exclusive bool) (func(), error) {
	return func() {}, nil
}
