package database

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/page"
)

type CommandContentFactory struct {
}

func (c CommandContentFactory) InsertToSet(set page.CandleSetWithoutYear, candles common.CandleList) []command.CommandContent {
	years := candles.SplitByYear()
	result := make([]command.CommandContent, 0, len(years))
	for year, yearCandles := range years {
		newCmd := command.NewInsertCommand(page.CandleSet{
			CandleSetWithoutYear: set,
			Year:                 year,
		}, yearCandles.ToTimestampCandleList())
		result = append(result, &newCmd)
	}
	return result
}
