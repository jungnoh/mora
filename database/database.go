package database

import (
	"github.com/jungnoh/mora/database/memory"
	"github.com/jungnoh/mora/database/util"
)

type Database struct {
	config util.Config
	lock   util.LockSet
	mem    memory.Memory
	disk   Disk
}

func NewDatabase(config util.Config) *Database {
	db := Database{}
	db.config = config
	db.mem.Lock = &db.lock
	db.mem.Config = &db.config
	db.disk.Lock = &db.lock
	db.disk.Config = &db.config
	return &db
}
