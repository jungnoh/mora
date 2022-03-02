package storage

import (
	"time"

	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

func (s *Storage) EvictMemory(reason MemoryEvictionReason) MemoryEvictionResult {
	defer func() {
		s.resetEvictionChan <- true
	}()

	result := MemoryEvictionResult{}
	pageCount, thresholdHitCount := s.memory.StatsForEviction(s.config.MaxMemoryPages)
	evictedCount := 0
	result.PagesCountBeforeEvict = pageCount

	if pageCount <= s.config.MaxMemoryPages {
		log.Info().Stringer("result", result).Stringer("reason", reason).Msg("eviction complete: nothing to evict")
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
		return
	})
	log.Info().Stringer("result", result).Stringer("reason", reason).Msg("eviction complete")
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

func (s *Storage) runPeriodicalEviction() {
	ticker := time.NewTicker(s.config.EvictionInterval)
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.EvictMemory(PeriodicalEvictionReason)
		case <-s.resetEvictionChan:
			ticker.Reset(s.config.EvictionInterval)
		}
	}
}
