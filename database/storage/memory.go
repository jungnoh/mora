package storage

import (
	"fmt"

	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type MemoryEvictionResult struct {
	PageCountBeforeEvict int
	EvictedCount         int
	AccessedPageCount    int
	Error                error
}

func (s *Storage) EvictMemory() MemoryEvictionResult {
	result := MemoryEvictionResult{}
	pageCount, thresholdHitCount := s.memory.StatsForEviction(s.config.MaxMemoryPages)
	evictedCount := 0
	result.PageCountBeforeEvict = pageCount

	if pageCount <= s.config.MaxMemoryPages {
		return result
	}

	s.memory.RangeForEviction(func(dirty bool, hitCount int, content *page.Page) (shouldEvict bool, quit bool, err error) {
		result.AccessedPageCount++
		if pageCount-evictedCount <= s.config.MaxMemoryPages {
			quit = true
			return
		}

		shouldEvict = hitCount <= thresholdHitCount
		if shouldEvict {
			evictedCount++
			result.EvictedCount++
		}
		if shouldEvict && dirty {
			if evictErr := s.evictPage(content); err != nil {
				err = errors.Wrap(evictErr, "failed to write page to disk")
			}
		}
		if err != nil {
			result.Error = err
		}
		fmt.Println(content.UniqueKey(), shouldEvict)
		return
	})
	return result
}

func (s *Storage) evictPage(content *page.Page) error {
	set := content.Header.ToCandleSet()
	header, exists, loadHeaderErr := s.diskLoadHeader(set)
	if loadHeaderErr != nil {
		return errors.Wrap(loadHeaderErr, "failed to load disk header")
	}
	if exists && header.LastTxId >= content.Header.LastTxId {
		return nil
	}
	if err := s.diskStore(set, content); err != nil {
		return errors.Wrap(err, "failed to writeback")
	}
	return nil
}
