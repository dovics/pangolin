package lsmt

import (
	"github.com/dovics/db"
	"github.com/dovics/db/utils/rbtree"
)

type memtable struct {
	data *rbtree.Tree

	valueType db.ValueType
	count     int
}

func (m *memtable) Set(key int64, value interface{}) {
	m.count++
	m.data.Insert(key, value)
}

func (m *memtable) Get(key int64) interface{} {
	return m.data.Find(key).GetValue()
}
