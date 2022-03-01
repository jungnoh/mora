package memory

import (
	"sync"

	"github.com/jungnoh/mora/page"
	"github.com/rs/zerolog/log"
)

type UnlockFunc func()

type memoryPage struct {
	accessLock sync.RWMutex
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
	return func() {
		d.logLocking("S", "Unlocking")
		d.accessLock.RUnlock()
	}
}

func (d *memoryPage) lockX() UnlockFunc {
	d.logLocking("X", "Trying to lock")
	d.accessLock.Lock()
	d.logLocking("X", "Locked")
	return func() {
		d.logLocking("X", "Unlocking")
		d.accessLock.Unlock()
	}
}
