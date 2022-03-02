package util

import "time"

type Config struct {
	Directory string `json:"directory"`

	MaxMemoryPages   int
	EvictionInterval time.Duration
}
