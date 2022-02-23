package common

import (
	"time"
)

type CandleList []Candle

type TimestampCandleList []TimestampCandle

func (t CandleList) ToTimestampCandleList() TimestampCandleList {
	result := make(TimestampCandleList, 0, len(t))
	for _, v := range t {
		result = append(result, v.ToTimestampCandle())
	}
	return result
}

func (t TimestampCandleList) ToCandleList() CandleList {
	result := make(CandleList, 0, len(t))
	for _, v := range t {
		result = append(result, v.ToCandle())
	}
	return result
}

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

func (c Candle) ToTimestampCandle() TimestampCandle {
	return TimestampCandle{
		TimelessCandle: c.TimelessCandle,
		Timestamp:      c.Timestamp.Unix(),
	}
}

type TimestampCandle struct {
	TimelessCandle
	Timestamp int64
}

func (c TimestampCandle) ToCandle() Candle {
	return Candle{
		TimelessCandle: c.TimelessCandle,
		Timestamp:      time.Unix(c.Timestamp, 0).UTC(),
	}
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
