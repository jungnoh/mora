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

func (d *memoryPage) logLocking(mode, message string) {
	log.Debug().Str("key", d.contentKey()).Str("set", "memory").Str("mode", mode).Msg(message)
}

func (d *memoryPage) lockS() UnlockFunc {
	d.logLocking("S", "Trying to lock")
	d.accessLock.RLock()
	d.logLocking("S", "Locked")

	unlocked := false
	return func() {
		if unlocked {
			return
		}
		unlocked = true
		d.logLocking("S", "Unlocking")
		d.accessLock.RUnlock()
	}
}

func (d *memoryPage) lockX() UnlockFunc {
	d.logLocking("X", "Trying to lock")
	d.accessLock.Lock()
	d.logLocking("X", "Locked")
	d.dirty = true

	unlocked := false
	return func() {
		if unlocked {
			return
		}
		unlocked = true
		d.logLocking("X", "Unlocking")
		d.accessLock.Unlock()
	}
}

func (d *memoryPage) lockForFlush() UnlockFunc {
	d.logLocking("F", "Trying to lock")
	d.accessLock.Lock()
	d.logLocking("F", "Locked")

	unlocked := false
	return func() {
		if unlocked {
			return
		}
		unlocked = true
		d.logLocking("F", "Unlocking")
		d.accessLock.Unlock()
	}
}
