package memory

import (
	"sync"

	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/page"
)

type pageMap struct {
	data sync.Map
	// TODO: add eviction
}

func (p *pageMap) Has(set common.UniqueKeyable) bool {
	_, ok := p.data.Load(set.UniqueKey())
	return ok
}

func (p *pageMap) Get(set common.UniqueKeyable) (*memoryPage, bool) {
	result, ok := p.data.Load(set.UniqueKey())
	if ok {
		return result.(*memoryPage), true
	}
	return nil, false
}

func (p *pageMap) AddIfNew(set common.UniqueKeyable, content *page.Page) (added bool) {
	if content == nil {
		_, loaded := p.data.LoadOrStore(set.UniqueKey(), &memoryPage{
			content: nil,
		})
		added = !loaded
	} else {
		copied := content.Copy()
		loadedPage, loaded := p.data.LoadOrStore(set.UniqueKey(), &memoryPage{
			content: &copied,
		})
		added = !loaded
		if loaded && loadedPage.(*memoryPage).content == nil {
			loadedPage.(*memoryPage).content = &copied
		}
	}
	return
}
