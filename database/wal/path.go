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
			result = append(result, path.Join(directory, file.Name()))
		}
	}
	return result, nil
}

func (w WalFileResolver) NewFile(txid uint64) (*os.File, error) {
	filename, err := w.ensureNewFile(txid)
	if err != nil {
		return nil, err
	}
	return os.Create(path.Join(w.dir(), filename))
}

func (w WalFileResolver) dir() string {
	return path.Join(w.Config.Directory, "wal")
}

func (w WalFileResolver) ensureNewFile(txid uint64) (string, error) {
	now := time.Now().UnixMilli()
	for {
		filename := w.makeFilename(now, txid)
		exists, err := util.FileExists(path.Join(w.dir(), filename))
		if err != nil {
			return "", err
		}
		if !exists {
			return filename, nil
		}
		now++
	}
}

func (w WalFileResolver) makeFilename(now int64, txid uint64) string {
	return fmt.Sprintf("%s%d%05d%s", walFilePrefix, now, txid%100000, walFileSuffix)
}

func (w WalFileResolver) filenameIsWalLog(name string) bool {
	return strings.HasPrefix(name, walFilePrefix) && strings.HasSuffix(name, walFileSuffix)
}
