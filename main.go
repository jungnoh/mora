package main

import (
	"flag"
	"sync"
	"time"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

func demo() {
	cfg := util.Config{
		Directory:        "/Users/mac/db",
		MaxMemoryPages:   2,
		EvictionInterval: 60 * time.Second,
	}
	db, err := database.NewDatabase(cfg)
	if err != nil {
		panic(err)
	}

	wg := sync.WaitGroup{}
	testMe := func(now time.Time, code string) {
		wg.Add(1)
		go func() {
			defer wg.Done()
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
			accessor.AddRead(targetSet)
			if err := accessor.Start(); err != nil {
				panic(err)
			}

			_, err = accessor.Get(targetSet)
			if err != nil {
				panic(err)
			}
			// fmt.Println(pg.Body)
			accessor.Rollback()
			log.Info().Msg("DONE")
		}()
	}

	testMe(time.Date(2021, time.January, 1, 0, 0, 0, 0, time.UTC), "ETH")
	testMe(time.Date(2021, time.July, 1, 0, 0, 0, 0, time.UTC), "BTC")
	testMe(time.Date(2022, time.January, 1, 0, 0, 0, 0, time.UTC), "BTC")
	testMe(time.Date(2022, time.July, 1, 0, 0, 0, 0, time.UTC), "BTC")
	wg.Wait()
	time.Sleep(2 * time.Second)
	// db.Storage.EvictMemory(storage.UserTriggerEvictionReason)
	// db.Storage.EvictMemory(storage.UserTriggerEvictionReason)

	db.Storage.FlushWal()
	<-make(chan bool)
}

func main() {
	debug := flag.Bool("debug", false, "sets log level to debug")
	flag.Parse()

	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	if *debug {
		SetupConsoleLogger()
		zerolog.SetGlobalLevel(zerolog.DebugLevel)
	}

	demo()
}
