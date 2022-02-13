package page

import (
	"io"

	"github.com/jungnoh/mora/common"
	"github.com/pkg/errors"
)

type PageBodyBlockList []PageBodyBlock

func NewPageBodyBlockList(year uint16, candles []common.Candle) PageBodyBlockList {
	result := make(PageBodyBlockList, len(candles))
	for i := 0; i < len(candles); i++ {
		result[i] = NewPageBodyBlock(year, candles[i])
	}
	return result
}

// Implmentation for sort.Interface

func (c PageBodyBlockList) Len() int {
	return len(c)
}

func (c PageBodyBlockList) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c PageBodyBlockList) Less(i, j int) bool {
	return c[i].Timestamp < c[j].Timestamp
}

func (c PageBodyBlockList) CreateIndex() (PageIndex, error) {
	dailyCount := make(PageIndex, INDEX_COUNT)
	index := make(PageIndex, INDEX_COUNT)

	for _, block := range c {
		day := block.TimestampOffset / 86400
		if day > 365 {
			return PageIndex{}, errors.New("block TimestampOffset out of bounds")
		}
		dailyCount[day]++
	}
	index.ApplyDailyCount(dailyCount)
	return index, nil
}

// Implementation for common.BinaryReadWriter

func (p *PageBodyBlockList) Read(count uint32, r io.Reader) error {
	*p = make([]PageBodyBlock, 0, count)
	for i := uint32(0); i < count; i++ {
		block := PageBodyBlock{}
		if err := block.Read(0, r); err != nil {
			return err
		}
		*p = append(*p, block)
	}
	return nil
}

func (c *PageBodyBlockList) Write(w io.Writer) error {
	for _, block := range *c {
		if err := block.Write(w); err != nil {
			return err
		}
	}
	return nil
}
