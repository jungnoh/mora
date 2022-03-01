package main

import (
	"fmt"
	"time"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
)

func main() {
	cfg := util.Config{
		Directory:      "/Users/mac/db",
		MaxCleanBlocks: 2,
	}
	db, err := database.NewDatabase(cfg)
	if err != nil {
		panic(err)
	}

	testMe := func(now time.Time, code string) {
		cds := make(common.CandleList, 1000)
		for i := 0; i < 1000; i++ {
			cds[i] = common.Candle{
				TimelessCandle: common.TimelessCandle{
					Open:   float64(100.0 + 5*i),
					High:   float64(101.0 + 5*i),
					Low:    float64(102.0 + 5*i),
					Close:  float64(103.0 + 5*i),
					Volume: float64(104.0 + 5*i),
				},
				Timestamp: now.AddDate(0, 0, i),
			}
		}

		err = db.Write(page.CandleSetWithoutYear{
			MarketCode:   "UPBIT",
			Code:         code,
			CandleLength: 60,
		}, cds)

		accessor, err := db.Storage.Access()
		if err != nil {
			panic(err)
		}
		targetSet := page.CandleSet{
			Year: uint16(now.Year()),
			CandleSetWithoutYear: page.CandleSetWithoutYear{
				MarketCode:   "UPBIT",
				Code:         code,
				CandleLength: 60,
			},
		}
		if err := accessor.AddRead(targetSet); err != nil {
			panic(err)
		}
		accessor.Start()
		pg, err := accessor.Get(targetSet)
		if err != nil {
			panic(err)
		}
		fmt.Println(pg.Body)
		accessor.Rollback()
	}

	go testMe(time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC), "ETH")
	// go testMe(time.Date(2021, time.July, 1, 0, 0, 0, 0, time.UTC), "BTC")
	// go testMe(time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC), "BTC")
	// go testMe(time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC), "BTC")
	<-make(chan bool)

	db.Storage.FlushWal()
	<-make(chan bool)
}
