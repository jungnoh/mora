package page

import (
	"fmt"

	"github.com/pkg/errors"
)

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
}

type CandleSet struct {
	CandleSetWithoutYear
	Year uint16
}

func (p CandleSet) IsZero() bool {
	return p.Year == 0
}

func (p CandleSet) UniqueKey() string {
	if p.IsZero() {
		panic(errors.New("cannot determine key of zero set"))
	}
	return fmt.Sprintf("%s^%s^%d^%d", p.MarketCode, p.Code, p.CandleLength, p.Year)
}
