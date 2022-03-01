package disk

import "github.com/rs/zerolog/log"

type UnlockFunc func()

func (d *Disk) lockS(key string) UnlockFunc {
	log.Debug().Str("key", key).Str("set", "disk").Str("mode", "S").Msg("Trying to lock")
	lock := d.accessLock.Get(key)
	lock.RLock()
	log.Debug().Str("key", key).Str("set", "disk").Str("mode", "S").Msg("Locked")
	return func() {
		log.Debug().Str("key", key).Str("set", "disk").Str("mode", "S").Msg("Unlocking")
		lock.RUnlock()
	}
}

func (d *Disk) lockX(key string) UnlockFunc {
	log.Debug().Str("key", key).Str("set", "disk").Str("mode", "X").Msg("Trying to lock")
	lock := d.accessLock.Get(key)
	lock.RLock()
	log.Debug().Str("key", key).Str("set", "disk").Str("mode", "X").Msg("Locked")
	return func() {
		log.Debug().Str("key", key).Str("set", "disk").Str("mode", "X").Msg("Unlocking")
		lock.RUnlock()
	}
}
