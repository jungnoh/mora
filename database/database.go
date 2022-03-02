package database

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/storage"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type Database struct {
	config  util.Config
	Storage *storage.Storage
}

func NewDatabase(config util.Config) (*Database, error) {
	db := Database{}
	db.config = config
	db.Storage = storage.NewStorage(&db.config)

	return &db, nil
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
	if err := accessor.Start(); err != nil {
		return errors.Wrapf(err, "failed to start")
	}
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
