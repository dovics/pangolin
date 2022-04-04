package pangolin

import (
	"encoding/json"
	"sort"
	"strings"
)

type Entry struct {
	Key   int64
	Value interface{}
	Type  ValueType
	Tags  []string
}

type ValueType int

const (
	UnknownType ValueType = iota
	IntType
	FloatType
	StringType

	TypeCount
)

func (e *Entry) Size() uint64 {
	switch e.Type {
	case IntType, FloatType:
		return 8
	case StringType:
		return uint64(len(e.Value.(string)) * 8)
	default:
		data, _ := json.Marshal(e.Value)
		return uint64(len(data) * 8)
	}
}

func (e *Entry) Index() string {
	indexBuilder := &strings.Builder{}
	sort.Strings(e.Tags)
	for i, tag := range e.Tags {
		indexBuilder.WriteString(tag)
		if i != len(e.Tags)-1 {
			indexBuilder.WriteRune(',')
		}
	}

	return indexBuilder.String()
}

type QueryFilter struct {
	Type ValueType
	Tags []string
}

func (f *QueryFilter) Index() string {
	indexBuilder := &strings.Builder{}
	sort.Strings(f.Tags)
	for i, tag := range f.Tags {
		indexBuilder.WriteString(tag)
		if i != len(f.Tags)-1 {
			indexBuilder.WriteRune(',')
		}
	}

	return indexBuilder.String()
}

func ContainTags(index string, tags []string) bool {
	for _, tag := range tags {
		if !strings.Contains(index, tag) {
			return false
		}
	}

	return true
}
