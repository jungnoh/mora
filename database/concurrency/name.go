package concurrency

import (
	"bytes"
	"hash/fnv"
	"strconv"

	"github.com/jungnoh/mora/page"
)

func NewResourceNamePart(value string) ResourceNamePart {
	h := fnv.New64()
	h.Write([]byte(value))
	return ResourceNamePart{
		Value:     value,
		hashValue: h.Sum64(),
	}
}

func NewResourceName(parts []ResourceNamePart) ResourceName {
	h := fnv.New64()
	for _, part := range parts {
		h.Write([]byte(part.Value))
	}
	return ResourceName{
		Parts:     parts,
		hashValue: h.Sum64(),
	}
}

type ResourceNamePart struct {
	Value     string
	hashValue uint64
}

func (r ResourceNamePart) Hash() uint64 {
	return r.hashValue
}

func (r ResourceNamePart) String() string {
	return r.Value
}

type ResourceName struct {
	Parts     []ResourceNamePart
	hashValue uint64
}

func (r ResourceName) Hash() uint64 {
	return r.hashValue
}

func (r ResourceName) LastPart() ResourceNamePart {
	return ResourceNamePart(r.Parts[len(r.Parts)-1])
}

func (r ResourceName) CreateChild(key ResourceNamePart) ResourceName {
	newParts := make([]ResourceNamePart, len(r.Parts)+1)
	copy(newParts, r.Parts)
	newParts[len(r.Parts)] = key
	return NewResourceName(newParts)
}

func (r ResourceName) String() string {
	var buf bytes.Buffer
	buf.WriteString("<")
	for i, arg := range r.Parts {
		if i > 0 {
			buf.WriteString(", ")
		}
		buf.WriteString(arg.Value)
	}
	buf.WriteString(">")
	return buf.String()
}

func NewMarketResourceName(marketCode string) ResourceName {
	return NewResourceName([]ResourceNamePart{
		NewResourceNamePart(marketCode),
	})
}

func NewCodeResourceName(marketCode, code string) ResourceName {
	return NewResourceName([]ResourceNamePart{
		NewResourceNamePart(marketCode),
		NewResourceNamePart(code),
	})
}

func NewSetResourceName(set page.CandleSet) ResourceName {
	return NewResourceName([]ResourceNamePart{
		NewResourceNamePart(set.MarketCode),
		NewResourceNamePart(set.Code),
		NewResourceNamePart(strconv.Itoa(int(set.Year))),
	})
}
