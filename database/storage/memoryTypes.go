package storage

import "fmt"

type MemoryEvictionReason int

func (m MemoryEvictionReason) String() string {
	switch m {
	case UserTriggerEvictionReason:
		return "USER"
	case PeriodicalEvictionReason:
		return "PERIODICAL"
	case AfterWalFlushEvictionReason:
		return "AFTER_WAL_FLUSH"
	default:
		return "UNKNOWN"
	}
}

const (
	UserTriggerEvictionReason   MemoryEvictionReason = 1
	PeriodicalEvictionReason    MemoryEvictionReason = 2
	AfterWalFlushEvictionReason MemoryEvictionReason = 3
)

type MemoryEvictionResult struct {
	PagesCountBeforeEvict int
	EvictedCount          int
	AccessedPageCount     int
	Error                 error
}

func (m MemoryEvictionResult) String() string {
	if m.Error != nil {
		return fmt.Sprintf("EvictionResult(PagesBeforeEvict=%d, Accessed=%d, Evicted=%d, Err=%+v)", m.PagesCountBeforeEvict, m.AccessedPageCount, m.EvictedCount, m.Error)
	}
	return fmt.Sprintf("EvictionResult(PagesBeforeEvict=%d, Accessed=%d, Evicted=%d)", m.PagesCountBeforeEvict, m.AccessedPageCount, m.EvictedCount)
}
