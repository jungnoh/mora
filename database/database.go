package database

import (
	"context"
	"fmt"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/storage"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
)

type Database struct {
	config          util.Config
	Storage         *storage.Storage
	evcitedNotiChan chan *page.Page
	ctx             context.Context
	ctxCancel       context.CancelFunc
}

func NewDatabase(config util.Config) (*Database, error) {
	db := Database{}
	db.ctx, db.ctxCancel = context.WithCancel(context.Background())
	db.config = config
	db.Storage = storage.NewStorage(&db.config)
	db.evcitedNotiChan = make(chan *page.Page)

	go db.evict()
	return &db, nil
}

func (d *Database) evict() {
	for {
		select {
		case <-d.ctx.Done():
			return
		case pg := <-d.evcitedNotiChan:
			err := d.execEvict(pg)
			if err != nil {
				log.Panic().Err(err).Msg("Eviction failed!")
			}
		}
	}
}

func (d *Database) execEvict(pg *page.Page) error {
	fmt.Println(pg.UniqueKey())
	return nil
}

func (d *Database) Execute(commands []command.CommandContent) error {
	accessor, err := d.Storage.Access()
	if err != nil {
		return err
	}
	defer accessor.RollbackIfActive()

	for _, cmd := range commands {
		for _, set := range cmd.TargetSets() {
			accessor.AddWrite(set)
		}
	}
	accessor.Start()
	for _, cmd := range commands {
		if err := accessor.Execute(cmd); err != nil {
			return errors.Wrapf(err, "failed to execute command '%s'", cmd.String())
		}
	}
	if err := accessor.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit")
	}
	return nil
}

// High level commands
func (d *Database) Write(set page.CandleSetWithoutYear, candles common.CandleList) error {
	commands := CommandContentFactory{}.InsertToSet(set, candles)
	return d.Execute(commands)
}
