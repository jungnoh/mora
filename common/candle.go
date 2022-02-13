package common

import (
	"time"
)

type CandleList []Candle

type TimelessCandle struct {
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
	BitFields uint32
}

type Candle struct {
	TimelessCandle
	Timestamp time.Time
}

type TimestampCandle struct {
	TimelessCandle
	Timestamp int64
}

func (c CandleList) Len() int {
	return len(c)
}
func (c CandleList) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}
func (c CandleList) Less(i, j int) bool {
	return c[i].Timestamp.Before(c[j].Timestamp)
}

func (c CandleList) SplitByYear() map[uint16]CandleList {
	years := make(map[uint16]CandleList)
	for i := range c {
		year := uint16(c[i].Timestamp.Year())
		if _, ok := years[year]; !ok {
			years[year] = make(CandleList, 0)
		}
		years[year] = append(years[year], c[i])
	}
	return years
}
