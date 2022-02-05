package page

import (
	"io"
	"sort"

	"github.com/jungnoh/mora/common"
	"github.com/pkg/errors"
)

type Page struct {
	Header PageHeader
	Body   PageBodyBlockList
}

func ReadPage(r io.Reader) (Page, error) {
	newPage := Page{}
	header, err := ReadPageHeader(r)
	if err != nil {
		return Page{}, errors.Wrap(err, "failed to read page header")
	}
	newPage.Header = header

	blocks := make([]PageBodyBlock, 0, header.Count)
	for i := uint32(0); i < header.Count; i++ {
		block, err := ReadPageBodyBlock(header, r)
		if err != nil {
			return Page{}, errors.Wrap(err, "failed to read page body")
		}
		blocks = append(blocks, block)
	}
	newPage.Body = blocks
	return newPage, nil
}

func (p *Page) Add(candles common.CandleList) error {
	sort.Sort(candles)

	firstInRange := p.Header.TimestampInPageRange(candles[0].Timestamp.Unix())
	lastInRange := p.Header.TimestampInPageRange(candles[len(candles)-1].Timestamp.Unix())
	if !(firstInRange && lastInRange) {
		return errors.New("candle timestamp is not in range")
	}

	if !candles[0].Timestamp.Before(p.Header.GetFirstTime()) {
		return p.append(candles)
	} else {
		return p.merge(candles)
	}
}

func (p *Page) append(candles common.CandleList) error {
	blocks := NewPageBodyBlockList(p.Header.Year, candles)
	p.Header.Count += uint32(len(blocks))
	p.Header.EndOffset = blocks[len(blocks)-1].TimestampOffset

	dailyCounts := make(PageIndex, INDEX_COUNT)
	for _, block := range blocks {
		dailyCounts[block.TimestampOffset/86400]++
	}
	p.Header.Index.ApplyDailyCount(dailyCounts)
	p.Body = append(p.Body, blocks...)

	return nil
}

func (p *Page) merge(candles common.CandleList) error {
	blocks := NewPageBodyBlockList(p.Header.Year, candles)
	if newStartOffset := blocks[0].TimestampOffset; newStartOffset < p.Header.StartOffset {
		p.Header.StartOffset = newStartOffset
	}
	if newEndOffset := blocks[len(blocks)-1].TimestampOffset; newEndOffset > p.Header.EndOffset {
		p.Header.EndOffset = newEndOffset
	}

	dailyCounts := make(PageIndex, INDEX_COUNT)
	newBody := make(PageBodyBlockList, 0, len(p.Body)+len(blocks))
	oldIndex, newIndex := 0, 0
	for oldIndex < len(p.Body) && newIndex < len(blocks) {
		oldOffset := p.Body[oldIndex].TimestampOffset
		newOffset := blocks[newIndex].TimestampOffset
		if oldOffset < newOffset {
			newBody = append(newBody, p.Body[oldIndex])
			dailyCounts[oldOffset/86400]++
			oldIndex++
		} else if oldOffset > newOffset {
			newBody = append(newBody, blocks[newIndex])
			dailyCounts[newOffset/86400]++
			newIndex++
		} else {
			newBody = append(newBody, blocks[newIndex])
			dailyCounts[newOffset/86400]++
			newIndex++
			oldIndex++
		}
	}
	for oldIndex < len(p.Body) {
		newBody = append(newBody, p.Body[oldIndex])
		dailyCounts[p.Body[oldIndex].TimestampOffset/86400]++
		oldIndex++
	}
	for newIndex < len(blocks) {
		newBody = append(newBody, blocks[newIndex])
		dailyCounts[blocks[newIndex].TimestampOffset/86400]++
		newIndex++
	}

	p.Header.Index = make(PageIndex, INDEX_COUNT)
	p.Header.Index.ApplyDailyCount(dailyCounts)
	p.Body = newBody
	p.Header.Count = uint32(len(newBody))

	return nil
}
