package memory

import (
	"github.com/jungnoh/mora/common"
	"github.com/jungnoh/mora/page"
)

func (m *Memory) Write(set page.CandleSet, candles common.CandleList) error {
	page := m.Access(set.UniqueKey())
	return page.Add(candles)
}
