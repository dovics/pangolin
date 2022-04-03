package lsmt

import (
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"strings"

	"github.com/dovics/db"
	"github.com/dovics/db/compress"
	"github.com/dovics/db/utils/rbtree"
)

type memtable struct {
	blocks [db.TypeCount]map[string]*memblock
	size   uint64

	minKey int64
	maxKey int64
}

func NewMemtable() *memtable {
	m := &memtable{minKey: -1, maxKey: -1}
	for i := 0; i < int(db.TypeCount); i++ {
		m.blocks[i] = make(map[string]*memblock)
	}

	return m
}

func (m *memtable) insert(e *db.Entry) error {
	indexBuilder := &strings.Builder{}
	sort.Strings(e.Tags)
	for i, tag := range e.Tags {
		indexBuilder.WriteString(tag)
		if i != len(e.Tags)-1 {
			indexBuilder.WriteRune(',')
		}
	}

	index := indexBuilder.String()

	table, ok := m.blocks[e.Type][index]
	if !ok {
		table = &memblock{data: rbtree.New(), count: 0}
		m.blocks[e.Type][index] = table
	}

	table.set(e.Key, e.Value)
	m.size += e.Size()

	if m.minKey == -1 || e.Key < m.minKey {
		m.minKey = e.Key
	}

	if m.maxKey == -1 || e.Key > m.maxKey {
		m.maxKey = e.Key
	}

	return nil
}

func (m *memtable) getRange(startTime, endTime int64, filter *db.QueryFilter) ([]interface{}, error) {
	if endTime < m.minKey || startTime > m.maxKey {
		return nil, nil
	}

	result := []interface{}{}
	if filter.Type != db.UnknownType {
		for i, block := range m.blocks[filter.Type] {
			if !containTags(i, filter.Tags) {
				continue
			}

			result = append(result, block.getRange(startTime, endTime)...)
		}

		return result, nil
	}

	for _, indexMap := range m.blocks {
		for i, block := range indexMap {
			if !containTags(i, filter.Tags) {
				continue
			}

			result = append(result, block.getRange(startTime, endTime)...)
		}
	}

	return result, nil
}

func containTags(index string, tags []string) bool {
	for _, tag := range tags {
		if !strings.Contains(index, tag) {
			return false
		}
	}

	return true
}
func (m *memtable) write(w io.Writer) error {
	indexes := []*index{}
	currentOffset := uint32(0)
	for t := 0; t < int(db.TypeCount); t++ {
		for i, table := range m.blocks[t] {
			length, err := table.write(w)
			if err != nil {
				return err
			}

			indexes = append(indexes, &index{
				index:  i,
				t:      table.valueType,
				count:  uint32(table.count),
				offset: currentOffset,
				length: uint32(length),
			})

			currentOffset += uint32(length)
		}

	}

	if err := writeHeader(w, indexes); err != nil {
		return err
	}

	return nil
}

type memblock struct {
	data *rbtree.Tree

	valueType db.ValueType
	count     int
}

func (m *memblock) set(key int64, value interface{}) {
	m.count++
	m.data.Insert(key, value)
}

func (m *memblock) get(key int64) interface{} {
	return m.data.Find(key).GetValue()
}

func (m *memblock) getRange(startTime, endTime int64) []interface{} {
	nodes := m.data.GetRange(startTime, endTime)
	result := make([]interface{}, len(nodes))

	for i, node := range nodes {
		result[i] = node.GetValue()
	}

	return result
}

// +---------------+---------------+----------------+----------------+---------------+
// |               |               |                |                |               |
// |   valueType   |   timeLength  |     times      |   valueLength  |     values    |
// |               |               |                |                |               |
// +---------------+---------------+----------------+----------------+---------------+

func (m *memblock) write(w io.Writer) (int, error) {
	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, uint32(m.valueType))
	if _, err := w.Write(buffer); err != nil {
		return 0, err
	}

	timeEncoder := compress.NewTimeEncoder(m.count)
	valueEncoder := NewEncoder(m.valueType, m.count)

	iter := rbtree.NewTreeIter(m.data)
	for k, v := iter.Next(); iter.HasNext(); k, v = iter.Next() {
		timeEncoder.Write(k)
		if err := valueEncoder.Write(v); err != nil {
			return 0, err
		}
	}

	timeData, err := timeEncoder.Bytes()
	if err != nil {
		return 0, err
	}

	binary.BigEndian.PutUint32(buffer, uint32(len(timeData)))
	if _, err := w.Write(buffer); err != nil {
		return 0, err
	}

	timeLength, err := w.Write(timeData)
	if err != nil {
		return 0, err
	}

	valueData, err := valueEncoder.Bytes()
	if err != nil {
		return 0, err
	}

	binary.BigEndian.PutUint32(buffer, uint32(len(valueData)))
	if _, err := w.Write(buffer); err != nil {
		return 0, err
	}

	valueLength, err := w.Write(valueData)
	if err != nil {
		return 0, err
	}

	return timeLength + valueLength, nil
}

func read(reader io.Reader) (*memblock, error) {
	buffer := make([]byte, 4)
	if n, err := reader.Read(buffer); err != nil {
		return nil, err
	} else if n != len(buffer) {
		return nil, errors.New("no enough length")
	}

	valueType := binary.BigEndian.Uint32(buffer)

	if n, err := reader.Read(buffer); err != nil {
		return nil, err
	} else if n != len(buffer) {
		return nil, errors.New("no enough length")
	}

	timeLength := binary.BigEndian.Uint32(buffer)

	timeDecoder := &compress.TimeDecoder{}

	timeBuffer := make([]byte, timeLength)
	if n, err := reader.Read(timeBuffer); err != nil {
		return nil, err
	} else if n != len(timeBuffer) {
		return nil, errors.New("no enough length")
	}

	timeDecoder.Init(timeBuffer)

	if n, err := reader.Read(buffer); err != nil {
		return nil, err
	} else if n != len(buffer) {
		return nil, errors.New("no enough length")
	}

	valueLength := binary.BigEndian.Uint32(buffer)

	valueBuffer := make([]byte, valueLength)
	if n, err := reader.Read(valueBuffer); err != nil {
		return nil, err
	} else if n != len(valueBuffer) {
		return nil, errors.New("no enough length")
	}

	decoder := NewDecoder(db.ValueType(valueType))
	decoder.SetBytes(valueBuffer)

	tree := rbtree.New()
	count := 0
	for {
		time := timeDecoder.Read()
		value := decoder.Read()
		tree.Insert(time, value)
		count++

		if !decoder.Next() || !timeDecoder.Next() {
			break
		}
	}

	return &memblock{
		tree,
		db.ValueType(valueType),
		count,
	}, nil

}
