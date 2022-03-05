package concurrency

type resourceNamePart struct {
	hash  uint64
	value string
}

type ResourceName struct {
	parts []resourceNamePart
}
