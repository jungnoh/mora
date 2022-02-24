package memory

import (
	lru "github.com/hashicorp/golang-lru"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
)

type Memory struct {
	Config *util.Config
	Lock   *util.LockSet

	cache   *lru.Cache
	evicted chan<- *page.Page
}

func NewMemory(cacheSize int, evictedNotiChan chan *page.Page) (*Memory, error) {
	mem := &Memory{
		evicted: evictedNotiChan,
	}
	cache, err := lru.NewWithEvict(cacheSize, func(key, value interface{}) {
		castKey, keyErr := key.(string)
		castValue, valueErr := value.(*page.Page)
		if !keyErr || !valueErr {
			panic("wrong cache key or value type!")
		}
		mem.onEvicted(castKey, castValue)
	})
	if err != nil {
		return nil, err
	}
	mem.cache = cache
	return mem, nil
}

func (m *Memory) onEvicted(key string, value *page.Page) {
	m.evicted <- value
}

func (m *Memory) GetPage(key string) (value *page.Page, ok bool) {
	origValue, origOk := m.cache.Get(key)
	if !origOk {
		value, ok = nil, false
		return
	}
	ok = true
	value, ok = origValue.(*page.Page)
	return
}

func (m *Memory) Insert(value *page.Page) {
	key := value.UniqueKey()
	m.cache.Add(key, value)
}
