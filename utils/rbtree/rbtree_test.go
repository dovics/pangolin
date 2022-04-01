package rbtree

import (
	"testing"
)

func TestInsertAndDelete(t *testing.T) {
	tree := New()

	m := int64(0)
	n := int64(1000)
	for m < n {
		tree.Insert(m, m)
		m++
	}

	node := tree.Find(500)
	if node.data != 500 && node.key != 500 {
		t.Error("can't find current node")
	}

	for m > 0 {
		tree.Delete(m)
		m--
	}
}