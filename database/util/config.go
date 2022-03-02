package util

type Config struct {
	Directory        string `json:"directory" yaml:"directory"`
	MaxMemoryPages   int    `json:"max_memory_pages" yaml:"max_memory_pages"`
	EvictionInterval int    `json:"eviction_interval" yaml:"eviction_interval"`
}
