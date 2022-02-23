package disk

import (
	"fmt"
	"os"
	"path"

	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type Disk struct {
	Config *util.Config
	Lock   *util.LockSet
}

func (p *Disk) candleFolder(marketCode string, candleLength uint32) string {
	return path.Join(p.Config.Directory, fmt.Sprintf("%s/%d", marketCode, candleLength))
}

func (p *Disk) folder(set page.CandleSet) string {
	return path.Join(p.Config.Directory, fmt.Sprintf("%s/%d/%s", set.MarketCode, set.CandleLength, set.Code))
}

func (p *Disk) filePathFromSet(set page.CandleSet) string {
	return path.Join(p.Config.Directory, fmt.Sprintf("%s/%d/%s/%05d.ysf", set.MarketCode, set.CandleLength, set.Code, set.Year))
}

func (p *Disk) filePathFromHeader(header page.PageHeader) string {
	return path.Join(p.Config.Directory, fmt.Sprintf("%s/%d/%s/%05d.ysf", header.MarketCode, header.CandleLength, header.Code, header.Year))
}

func (d Disk) ReadHeader(set page.CandleSet) (page.PageHeader, error) {
	lock := d.Lock.Disk.Get(set.UniqueKey())
	lock.RLock()
	defer lock.RUnlock()

	path := d.filePathFromSet(set)
	f, err := os.Open(path)
	if err != nil {
		return page.PageHeader{}, errors.Wrap(err, "disk ReadHeader failed")
	}
	defer f.Close()
	header := page.PageHeader{}
	if err := header.Read(0, f); err != nil {
		return page.PageHeader{}, errors.Wrap(err, "disk ReadHeader failed")
	}
	return header, nil
}

func (d Disk) Read(set page.CandleSet) (page.Page, error) {
	lock := d.Lock.Disk.Get(set.UniqueKey())
	lock.RLock()
	defer lock.RUnlock()

	path := d.filePathFromSet(set)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return page.Page{}, nil
		}
		return page.Page{}, errors.Wrap(err, "disk Read failed")
	}
	defer f.Close()
	page := page.Page{}
	if err := page.Read(0, f); err != nil {
		return page, errors.Wrap(err, "disk Read failed")
	}
	return page, nil
}

func (d Disk) Write(content page.Page) error {
	lock := d.Lock.Disk.Get(content.UniqueKey())
	lock.Lock()
	defer lock.Unlock()

	path := d.filePathFromHeader(content.Header)
	if err := util.EnsureDirectoryOfFile(path); err != nil {
		return errors.Wrap(err, "disk file folder preparing failed")
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return errors.Wrap(err, "disk file open failed")
	}
	defer f.Close()
	if err := content.Header.Write(f); err != nil {
		return errors.Wrap(err, "disk header write failed")
	}
	if err := content.Body.Write(f); err != nil {
		return errors.Wrap(err, "disk body write failed")
	}
	return nil
}
