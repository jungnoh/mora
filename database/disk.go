package database

import (
	"fmt"
	"io/fs"
	"os"
	"path"

	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type Disk struct {
	Config *Config
	Lock   *LockSet
}

func (p *Disk) candleFolder(marketCode string, candleLength uint32) string {
	return path.Join(p.Config.Directory, fmt.Sprintf("%s/%d", marketCode, candleLength))
}

func (p *Disk) folder(set page.CandleSet) string {
	return path.Join(p.Config.Directory, fmt.Sprintf("%s/%d/%s", set.MarketCode, set.CandleLength, set.Code))
}

func (p *Disk) filePath(set page.CandleSet) string {
	return path.Join(p.Config.Directory, fmt.Sprintf("%s/%d/%s/%05d.ysf", set.MarketCode, set.CandleLength, set.Code, set.Year))
}

func (d Disk) ReadHeader(set page.CandleSet) (page.PageHeader, error) {
	lock := d.Lock.Disk.Get(set.UniqueKey())
	lock.RLock()
	defer lock.RUnlock()

	path := d.filePath(set)
	f, err := os.Open(path)
	if err != nil {
		return page.PageHeader{}, errors.Wrap(err, "disk ReadHeader failed")
	}
	defer f.Close()
	header, err := page.ReadPageHeader(f)
	if err != nil {
		return page.PageHeader{}, errors.Wrap(err, "disk ReadHeader failed")
	}
	return header, nil
}

func (d Disk) Read(set page.CandleSet) (page.Page, error) {
	lock := d.Lock.Disk.Get(set.UniqueKey())
	lock.RLock()
	defer lock.RUnlock()

	path := d.filePath(set)
	f, err := os.Open(path)
	if err != nil {
		return page.Page{}, errors.Wrap(err, "disk Read failed")
	}
	defer f.Close()
	result, err := page.ReadPage(f)
	if err != nil {
		return page.Page{}, errors.Wrap(err, "disk Read failed")
	}
	return result, nil
}

func (d Disk) Write(set page.CandleSet, content page.Page) error {
	lock := d.Lock.Disk.Get(set.UniqueKey())
	lock.Lock()
	defer lock.Unlock()

	path := d.filePath(set)
	f, err := os.OpenFile(path, 0755, fs.FileMode(os.O_RDWR|os.O_TRUNC|os.O_CREATE))
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
