package memory

import (
	"sync"

	"github.com/jungnoh/mora/page"
	"github.com/rs/zerolog/log"
)

type UnlockFunc func()

type memoryPage struct {
	accessLock sync.RWMutex
	dirty      bool
	content    *page.Page
}

func (d *memoryPage) contentKey() string {
	if d.content == nil {
		return ""
	}
	return d.content.UniqueKey()
}

func (d *memoryPage) logLocking(txId uint64, mode, message string) {
	log.Debug().Uint64("txId", txId).Str("key", d.contentKey()).Str("set", "memory").Str("mode", mode).Msg(message)
}

func (d *memoryPage) lockS(txId uint64) UnlockFunc {
	d.logLocking(txId, "S", "Trying to lock")
	d.accessLock.RLock()
	d.logLocking(txId, "S", "Locked")

	unlocked := false
	return func() {
		if unlocked {
			return
		}
		unlocked = true
		d.logLocking(txId, "S", "Unlocking")
		d.accessLock.RUnlock()
	}
}

func (d *memoryPage) lockX(txId uint64) UnlockFunc {
	d.logLocking(txId, "X", "Trying to lock")
	d.accessLock.Lock()
	d.logLocking(txId, "X", "Locked")
	d.dirty = true

	unlocked := false
	return func() {
		if unlocked {
			return
		}
		unlocked = true
		d.logLocking(txId, "X", "Unlocking")
		d.accessLock.Unlock()
	}
}

func (d *memoryPage) lockForFlush() UnlockFunc {
	d.logLocking(0, "F", "Trying to lock")
	d.accessLock.Lock()
	d.logLocking(0, "F", "Locked")

	unlocked := false
	return func() {
		if unlocked {
			return
		}
		unlocked = true
		d.logLocking(0, "F", "Unlocking")
		d.accessLock.Unlock()
	}
}
