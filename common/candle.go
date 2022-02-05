package common

import "time"

type CandleList []Candle

type Candle struct {
	Timestamp time.Time
	Open      float64
	High      float64
	Low       float64
	Close     float64
	Volume    float64
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
