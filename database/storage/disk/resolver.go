package disk

import (
	"fmt"
	"path"

	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
)

type filePathResolver struct {
	config *util.Config
}

func (f filePathResolver) buildFile(marketCode, code string, length uint32, year uint16) string {
	return path.Join(f.config.Directory, fmt.Sprintf("%s/%d/%s/%05d.ysf", marketCode, length, code, year))
}

func (f filePathResolver) buildFolder(marketCode, code string, length uint32) string {
	return path.Join(f.config.Directory, fmt.Sprintf("%s/%d/%s", marketCode, length, code))
}

func (f filePathResolver) CandleFolder(marketCode string, candleLength uint32) string {
	return path.Join(f.config.Directory, fmt.Sprintf("%s/%d", marketCode, candleLength))
}

func (f filePathResolver) FolderFromSet(set page.CandleSet) string {
	return f.buildFolder(set.MarketCode, set.Code, set.CandleLength)
}

func (f filePathResolver) FolderFromHeader(header page.PageHeader) string {
	return f.buildFolder(header.MarketCode, header.Code, header.CandleLength)
}

func (f filePathResolver) FileFromSet(set page.CandleSet) string {
	return f.buildFile(set.MarketCode, set.Code, set.CandleLength, set.Year)
}

func (f filePathResolver) FileFromHeader(header page.PageHeader) string {
	return f.buildFile(header.MarketCode, header.Code, header.CandleLength, header.Year)
}
