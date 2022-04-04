package lsmt

import (
	"bytes"
	"testing"

	db "github.com/dovics/pangolin"
	"github.com/dovics/pangolin/utils/rbtree"
)

func TestBlock(t *testing.T) {
	mt := &block{data: rbtree.New(), valueType: db.IntType}

	for i := int64(0); i < 10; i++ {
		mt.set(i, i)
	}

	buffer := new(bytes.Buffer)

	if _, err := mt.writeBlock(buffer); err != nil {
		t.Fatal(err)
	}

	t.Log(buffer.Bytes())

	newt, err := readBlock(buffer)
	if err != nil {
		t.Fatal(err)
	}

	if newt.count != mt.count {
		t.Error("wrong count")
	}
}
