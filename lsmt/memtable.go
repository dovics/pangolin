package lsmt

import "github.com/dovics/db/utils/rbtree"

type memtable struct {
	data *rbtree.Tree
	size int64
}

func (m *memtable) Set(key uint64, value interface{}) {
	m.data.Insert(key, value)
}

func (m *memtable) Get(key uint64) interface{} {
	return m.data.Find(key).GetValue()
}

func (m *memtable) Size() int64 {
	return m.size
}
