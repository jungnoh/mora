package database

import (
	"sort"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/page"
)

type CommandContentFactory struct {
}

func (c CommandContentFactory) InsertToSet(set page.CandleSetWithoutYear, candles common.CandleList) []command.CommandContent {
	years := candles.SplitByYear()
	result := make([]command.CommandContent, 0, len(years))

	yearKeys := make([]int, len(years))
	i := 0
	for k := range years {
		yearKeys[i] = int(k)
		i++
	}
	sort.Ints(yearKeys)

	for _, year := range yearKeys {
		yearCandles := years[uint16(year)]
		newCmd := command.NewInsertCommand(page.CandleSet{
			CandleSetWithoutYear: set,
			Year:                 uint16(year),
		}, yearCandles.ToTimestampCandleList())
		result = append(result, &newCmd)
	}
	return result
}
