package storage

import (
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

func (s *Storage) EvictMemory() {
	s.memory.RangeForEviction(func(dirty bool, content *page.Page) (shouldEvict bool, err error) {
		// TODO: Add eviction check logic
		shouldEvict = false
		set := content.Header.ToCandleSet()
		header, exists, loadHeaderErr := s.diskLoadHeader(set)
		if err != nil {
			err = errors.Wrap(loadHeaderErr, "failed to load disk header")
		}
		if !exists || header.LastTxId < content.Header.LastTxId {
			if evictErr := s.evictPage(set, content); err != nil {
				err = errors.Wrap(evictErr, "failed to write page to disk")
			}
		}
		shouldEvict = true
		return
	})
}

func (s *Storage) evictPage(set page.CandleSet, content *page.Page) error {
	return s.diskStore(set, content)
}
