package lsmt

import (
	"sort"
	"strings"

	"github.com/dovics/db"
)

func init() {
	db.Register("lsm", func() db.Engine {
		return &Storage{
			memtables: make(map[string]*memtable),
		}
	})
}

type Storage struct {
	memtables map[string]*memtable
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
		table = &memtable{size: 0}
		s.memtables[index] = table
	}

	table.Set(e.Key, e.Value)
	return nil
}
