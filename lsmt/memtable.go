package lsmt

import (
	"encoding/binary"
	"io"
	"sync"

	db "github.com/dovics/pangolin"
	"github.com/dovics/pangolin/utils/rbtree"
)

type memtable struct {
	mutex  sync.RWMutex
	blocks [db.TypeCount]map[string]*block
	size   uint64

	minKey int64
	maxKey int64
}

func NewMemtable() *memtable {
	m := &memtable{minKey: -1, maxKey: -1}
	for i := 0; i < int(db.TypeCount); i++ {
		m.blocks[i] = make(map[string]*block)
	}

	return m
}

func (m *memtable) insert(e *db.Entry) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	index := e.Index()

	table, ok := m.blocks[e.Type][index]
	if !ok {
		table = &block{data: rbtree.New(), count: 0, valueType: e.Type}
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
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	if endTime < m.minKey || startTime > m.maxKey {
		return nil, nil
	}

	result := []interface{}{}
	if filter != nil && filter.Type != db.UnknownType {
		for i, block := range m.blocks[filter.Type] {
			if !db.ContainTags(i, filter.Tags) {
				continue
			}

			result = append(result, block.getRange(startTime, endTime)...)
		}

		return result, nil
	}

	for _, indexMap := range m.blocks {
		for i, block := range indexMap {
			if filter != nil && !db.ContainTags(i, filter.Tags) {
				continue
			}

			result = append(result, block.getRange(startTime, endTime)...)
		}
	}

	return result, nil
}

func (m *memtable) write(w io.Writer) error {
	m.mutex.RLock()
	defer m.mutex.RUnlock()

	indexes := []*index{}
	currentOffset := uint32(0)
	for t := 0; t < int(db.TypeCount); t++ {
		for i, table := range m.blocks[t] {
			length, err := table.writeBlock(w)
			if err != nil {
				return err
			}

			indexes = append(indexes, &index{
				index:  i,
				t:      table.valueType,
				count:  uint32(table.count),
				min:    uint32(table.data.Min().(rbtree.TimestampItem).Time),
				max:    uint32(table.data.Max().(rbtree.TimestampItem).Time),
				offset: currentOffset,
				length: uint32(length),
			})

			currentOffset += uint32(length)
		}

	}

	if err := writeHeader(w, indexes); err != nil {
		return err
	}

	buffer := make([]byte, 4)
	binary.BigEndian.PutUint32(buffer, currentOffset)
	if _, err := w.Write(buffer); err != nil {
		return err
	}

	return nil
}
