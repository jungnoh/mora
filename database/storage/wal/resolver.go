package wal

import (
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"
	"time"

	"github.com/jungnoh/mora/database/util"
)

const walFilePrefix string = "wal."
const walFileSuffix string = ".log"

type WalFileResolver struct {
	Config *util.Config
}

func (w WalFileResolver) Counter() string {
	return path.Join(w.dir(), "counter")
}

func (w WalFileResolver) AllFiles() ([]string, error) {
	directory := w.dir()
	files, err := ioutil.ReadDir(directory)
	if err != nil {
		return []string{}, err
	}
	result := make([]string, 0, len(files))
	for _, file := range files {
		if !file.IsDir() && w.filenameIsWalLog(file.Name()) {
			result = append(result, file.Name())
		}
	}
	return result, nil
}

func (w WalFileResolver) NewFile(txid uint64) (*os.File, string, error) {
	filename, err := w.ensureNewFile(txid)
	if err != nil {
		return nil, "", err
	}
	fd, err := os.Create(path.Join(w.dir(), filename))
	return fd, filename, err
}

func (w WalFileResolver) FullPath(filename string) string {
	return path.Join(w.Config.Directory, "wal", filename)
}

func (w WalFileResolver) dir() string {
	return path.Join(w.Config.Directory, "wal")
}

func (w WalFileResolver) ensureNewFile(txId uint64) (string, error) {
	now := time.Now().UnixMilli()
	usedId := txId
	for {
		filename := w.makeFilename(now, usedId)
		exists, err := util.FileExists(path.Join(w.dir(), filename))
		if err != nil {
			return "", err
		}
		if !exists {
			return filename, nil
		}
		usedId++
	}
}

func (w WalFileResolver) makeFilename(now int64, id uint64) string {
	return fmt.Sprintf("%s%d%05d%s", walFilePrefix, now, id%100000, walFileSuffix)
}

func (w WalFileResolver) filenameIsWalLog(name string) bool {
	return strings.HasPrefix(name, walFilePrefix) && strings.HasSuffix(name, walFileSuffix)
}
