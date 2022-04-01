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
	memsize   uint64

	minKey int64
	maxKey int64
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
	s.memsize += e.Size()

	if e.Key < s.minKey {
		s.minKey = e.Key
	}

	if e.Key > s.maxKey {
		s.maxKey = e.Key
	}

	return s.SaveToFileIfNeeded()
}

func (s *Storage) SaveToFileIfNeeded() error {
	if s.memsize < s.option.MaxMemtableSize {
		return nil
	}

	file, err := os.Create(filepath.Join(s.option.WorkDir,
		strconv.FormatInt(s.minKey, 10)+"-"+strconv.FormatInt(s.maxKey, 10)))
	if file != nil {
		defer file.Close()
	}

	if err != nil {
		return err
	}

	totalOffsetBuffer := []uint32{}
	totalLengthBuffer := []uint32{}
	for t := 0; t < int(db.TypeCount); t++ {
		offsetBuffer := make([]uint32, len(s.memtables[t]))
		lengthBuffer := make([]uint32, len(s.memtables[t]))
		currentOffset, i := uint32(0), 0
		for _, table := range s.memtables[t] {
			offsetBuffer[i] = currentOffset

			length, err := table.Write(file)
			if err != nil {
				return err
			}

			lengthBuffer[i] = uint32(length)
			currentOffset += lengthBuffer[i]
		}

		totalOffsetBuffer = append(totalOffsetBuffer, offsetBuffer...)
		totalLengthBuffer = append(totalLengthBuffer, lengthBuffer...)
	}

	if err := s.WriteHeader(file, totalOffsetBuffer, totalLengthBuffer); err != nil {
		return err
	}

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

type Decoder interface {
	SetBytes(b []byte) error
	Next() bool
	Read() interface{}
	Error() error
}

func NewDecoder(t db.ValueType) Decoder {
	switch t {
	case db.IntType:
		return &compress.IntegerDecoder{}
	case db.FloatType:
		return &compress.FloatDecoder{}
	case db.StringType:
		return &compress.StringDecoder{}
	default:
		return &compress.StringDecoder{}
	}
}
