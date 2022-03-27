package lsmt

import (
	"errors"
	"sort"
	"strings"

	"github.com/dovics/db"
	"github.com/dovics/db/utils/rbtree"
)

func init() {
	db.Register("lsm", func(o interface{}) (db.Engine, error) {
		option, ok := o.(*Option)
		if !ok {
			return nil, errors.New("wrong option type")
		}

		return &Storage{
			option:    option,
			memtables: make(map[string]*memtable),
		}, nil
	})
}

type Option struct {
	WorkDir string

	CompressEnable     bool
	MaxMemtableSize    uint64
	MaxCompactFileSize int64
}

type Storage struct {
	option *Option

	memtables map[string]*memtable
	memsize   map[string]uint64
}

func (s *Storage) Insert(e *db.Entry) error {
	indexBuilder := &strings.Builder{}
	for i, tag := range e.Tags {
		indexBuilder.WriteString(tag)
		if i != len(e.Tags)-1 {
			indexBuilder.WriteRune(',')
		}
	}

	labels := make([]string, len(e.Lables))
	i := 0
	for key, value := range e.Lables {
		labels[i] = key + "=" + value
		i++
	}

	sort.Strings(labels)
	for i, label := range labels {
		indexBuilder.WriteString(label)
		if i != len(labels)-1 {
			indexBuilder.WriteRune(',')
		}
	}

	index := indexBuilder.String()

	table, ok := s.memtables[index]
	if !ok {
		table = &memtable{rbtree.New()}
		s.memtables[index] = table

	}

	table.Set(e.Key, e.Value)
	s.memsize[index] += e.Size

	return nil
}

func (s *Storage) SaveToFileIfNeeded(index string) {
	if s.memsize[index] < s.option.MaxMemtableSize {
		return
	}

}
