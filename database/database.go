package database

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/database/command"
	"github.com/jungnoh/mora/database/concurrency"
	"github.com/jungnoh/mora/database/storage"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type Database struct {
	config  util.Config
	Storage *storage.Storage
	Lock    *concurrency.DatabaseLock
}

func NewDatabase(config util.Config) (*Database, error) {
	db := Database{}
	db.config = config
	db.Storage = storage.NewStorage(&db.config)
	db.Lock = concurrency.NewDatabaseLock()

	return &db, nil
}
func (d *Database) Execute(commands []command.CommandContent) ([]interface{}, error) {
	accessor, err := d.Storage.Access()
	if err != nil {
		return []interface{}{}, err
	}
	tx := NewTransactionContext(&accessor, d.Lock)
	defer tx.RollbackIfActive()

	if err := tx.Start(); err != nil {
		return []interface{}{}, errors.Wrapf(err, "failed to start")
	}

	result := make([]interface{}, 0, len(commands))
	for _, cmd := range commands {
		cmdResult, err := tx.Execute(cmd)
		if err != nil {
			return []interface{}{}, errors.Wrapf(err, "failed to execute command '%s'", cmd.String())
		}
		result = append(result, cmdResult)
	}
	if err := tx.Commit(); err != nil {
		return []interface{}{}, errors.Wrap(err, "failed to commit")
	}
	return result, nil
}

// High level commands
func (d *Database) Write(set page.CandleSetWithoutYear, candles common.CandleList) ([]interface{}, error) {
	commands := CommandContentFactory{}.InsertToSet(set, candles)
	return d.Execute(commands)
}
