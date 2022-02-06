package page

import "fmt"

type PageIndex []uint32

func (p PageIndex) ApplyDailyCount(dailyCount PageIndex) {
	culSum := uint32(0)
	for i := 1; i < INDEX_COUNT; i++ {
		culSum += dailyCount[i-1]
		p[i] += culSum
	}
}

type CandleSetWithoutYear struct {
	MarketCode   string
	Code         string
	CandleLength uint32
	Year         uint16
}

type CandleSet struct {
	CandleSetWithoutYear
	Year uint16
}

func (p CandleSet) UniqueKey() string {
	return fmt.Sprintf("%s^%s^%d^%d", p.MarketCode, p.Code, p.CandleLength, p.Year)
}
