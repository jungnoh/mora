package memory

import (
	"fmt"
	"sync"

	lru "github.com/hashicorp/golang-lru"
	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
)

type Memory struct {
	Config *util.Config
	Lock   *util.LockSet

	cache   *lru.Cache
	evicted chan<- *page.Page

	memLock    sync.RWMutex
	refCounter map[string]int
}

func NewMemory(cacheSize int, evictedNotiChan chan *page.Page) (*Memory, error) {
	mem := &Memory{
		evicted:    evictedNotiChan,
		refCounter: make(map[string]int),
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
	ref := m.getRef(key)
	fmt.Println(ref)
	if ref > 0 {
		fmt.Println("[[[", key)
		m.Insert(value)
		fmt.Println("done")
	} else {
		m.evicted <- value
	}
}

func (m *Memory) GetPage(key string) (value *page.Page, ok bool) {
	m.memLock.Lock()
	defer m.memLock.Unlock()

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

func (m *Memory) getRef(key string) int {
	v, ok := m.refCounter[key]
	if ok {
		return v
	}
	return 0
}

func (m *Memory) Ref(key string) {
	m.memLock.Lock()
	defer m.memLock.Unlock()
	v, ok := m.refCounter[key]
	if ok {
		m.refCounter[key] = v + 1
	} else {
		m.refCounter[key] = 1
	}
}

func (m *Memory) FreeRef(key string) {
	m.memLock.Lock()
	defer m.memLock.Unlock()
	v, ok := m.refCounter[key]
	if ok {
		m.refCounter[key] = v - 1
	} else {
		m.refCounter[key] = 0
	}
}
