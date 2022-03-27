package lsmt

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/dovics/db"
	"github.com/dovics/db/compress"
	"github.com/dovics/db/utils/rbtree"
)

func init() {
	db.Register("lsm", func(o interface{}) (db.Engine, error) {
		if o == nil {
			o = defaultOption
		}

		option, ok := o.(*Option)
		if !ok {
			return nil, errors.New("wrong option type")
		}

		if err := os.Mkdir(option.WorkDir, 0750); err != nil {
			return nil, err
		}

		s := &Storage{
			option: option,
		}

		for i := 0; i < int(db.TypeCount); i++ {
			s.memtables[i] = make(map[string]*memtable)
			s.memsize[i] = make(map[string]uint64)
		}

		return s, nil
	})
}

type Option struct {
	WorkDir string

	CompressEnable     bool
	MaxMemtableSize    uint64
	MaxCompactFileSize int64
}

var defaultOption *Option = &Option{
	WorkDir:            "./lsm",
	CompressEnable:     true,
	MaxMemtableSize:    1024,
	MaxCompactFileSize: 1024,
}

type Storage struct {
	option *Option

	memtables [db.TypeCount]map[string]*memtable
	memsize   [db.TypeCount]map[string]uint64
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

	table, ok := s.memtables[e.Type][index]
	if !ok {
		table = &memtable{data: rbtree.New(), count: 0}
		s.memtables[e.Type][index] = table
	}

	table.Set(e.Key, e.Value)
	s.memsize[e.Type][index] += e.Size()
	return s.SaveToFileIfNeeded(e.Type, index)
}

func (s *Storage) SaveToFileIfNeeded(t db.ValueType, index string) error {
	if s.memsize[t][index] < s.option.MaxMemtableSize {
		return nil
	}

	table := s.memtables[t][index]
	timeEncoder := compress.NewTimeEncoder(table.count)
	valueEncoder := NewEncoder(t, table.count)

	iter := rbtree.NewTreeIter(table.data)
	for k, v := iter.Next(); iter.HasNext(); k, v = iter.Next() {
		timeEncoder.Write(k)
		if err := valueEncoder.Write(v); err != nil {
			return err
		}

	}

	minKey, maxKey := table.data.Min().GetKey(), table.data.Max().GetKey()
	file, err := os.Create(filepath.Join(s.option.WorkDir,
		strconv.FormatInt(minKey, 10)+"-"+strconv.FormatInt(maxKey, 10)))
	if err != nil {
		return err
	}

	timeData, err := timeEncoder.Bytes()
	if err != nil {
		return err
	}

	if _, err := file.Write(timeData); err != nil {
		return err
	}

	valueData, err := valueEncoder.Bytes()
	if err != nil {
		return err
	}

	if _, err := file.Write(valueData); err != nil {
		return err
	}

	delete(s.memsize[t], index)
	delete(s.memtables[t], index)
	return nil
}

type Encoder interface {
	Write(interface{}) error
	Bytes() ([]byte, error)
	Flush()
	Reset()
}

func NewEncoder(t db.ValueType, size int) Encoder {
	switch t {
	case db.IntType:
		return compress.NewIntegerEncoder(size)
	case db.FloatType:
		return compress.NewFloatEncoder()
	case db.StringType:
		return compress.NewStringEncoder(size)
	default:
		return compress.NewStringEncoder(size)
	}
}
