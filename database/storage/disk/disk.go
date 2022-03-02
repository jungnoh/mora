package disk

import (
	"os"

	"github.com/jungnoh/mora/database/util"
	"github.com/jungnoh/mora/page"
	"github.com/pkg/errors"
)

type Disk struct {
	filePath   filePathResolver
	accessLock util.RWMutexMap
}

func NewDisk(config *util.Config) Disk {
	return Disk{
		filePath:   filePathResolver{config: config},
		accessLock: util.NewRWMutexMap(),
	}
}

func (d *Disk) ReadHeader(set page.CandleSet) (page.PageHeader, error) {
	key := set.UniqueKey()
	unlock := d.lockS(key)
	defer unlock()

	path := d.filePath.FileFromSet(set)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return page.PageHeader{}, nil
		}
		return page.PageHeader{}, errors.Wrapf(err, "read header fail (key '%s')", key)
	}
	defer f.Close()
	header := page.PageHeader{}
	if err := header.Read(0, f); err != nil {
		return page.PageHeader{}, errors.Wrapf(err, "read header fail (key '%s')", key)
	}
	return header, nil
}

func (d *Disk) Read(set page.CandleSet) (page.Page, error) {
	key := set.UniqueKey()
	unlock := d.lockS(key)
	defer unlock()

	path := d.filePath.FileFromSet(set)
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return page.Page{}, nil
		}
		return page.Page{}, errors.Wrapf(err, "disk read fail (key '%s')", key)
	}
	defer f.Close()
	page := page.Page{}
	if err := page.Read(0, f); err != nil {
		return page, errors.Wrapf(err, "disk read fail (key '%s')", key)
	}
	return page, nil
}

func (d *Disk) Write(content page.Page) error {
	key := content.UniqueKey()
	unlock := d.lockX(key)
	defer unlock()

	path := d.filePath.FileFromHeader(content.Header)
	if err := util.EnsureDirectoryOfFile(path); err != nil {
		return errors.Wrapf(err, "folder preparing fail (key '%s')", key)
	}
	f, err := os.OpenFile(path, os.O_RDWR|os.O_TRUNC|os.O_CREATE, 0755)
	if err != nil {
		return errors.Wrapf(err, "open fail (key '%s')", key)
	}
	defer f.Close()
	if err := content.Header.Write(f); err != nil {
		return errors.Wrapf(err, "header write fail (key '%s')", key)
	}
	if err := content.Body.Write(f); err != nil {
		return errors.Wrapf(err, "body write fail (key '%s')", key)
	}
	return nil
}
