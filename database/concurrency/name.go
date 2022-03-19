package concurrency

import "strings"

type ResourceNamePart string

type ResourceName string

func (r ResourceName) Child(key ResourceNamePart) ResourceName {
	return r + "/" + ResourceName(key)
}

func (r ResourceName) LastPart() ResourceNamePart {
	lastIndex := strings.LastIndex(string(r), "/")
	if lastIndex == -1 {
		return ""
	}
	return ResourceNamePart(r[lastIndex+1:])
}
