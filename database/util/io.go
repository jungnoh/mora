package util

import (
	"errors"
	"os"
	"path"
)

func FileExists(path string) (bool, error) {
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func EnsureDirectoryOfFile(filePath string) error {
	return os.MkdirAll(path.Dir(filePath), 0755)
}
