package common

import (
	"sync"
	"time"
)

type TimeRange struct {
	Start time.Time `json:"start"`
	End   time.Time `json:"end"`
}

var yearTsLock sync.Mutex = sync.Mutex{}
var yearTsCache map[int]int64 = make(map[int]int64)

func GetStartOfYearTimestamp(year int) int64 {
	yearTsLock.Lock()
	defer yearTsLock.Unlock()
	if offset, ok := yearTsCache[year]; ok {
		return offset
	}
	yearTsCache[year] = time.Date(year, time.January, 1, 0, 0, 0, 0, time.UTC).Unix()
	return yearTsCache[year]
}
