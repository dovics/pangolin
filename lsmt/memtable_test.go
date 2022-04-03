package lsmt

import (
	"bytes"
	"testing"

	"github.com/dovics/db"
	"github.com/dovics/db/utils/rbtree"
)

func TestMemtableBackup(t *testing.T) {
	mt := &memblock{data: rbtree.New(), valueType: db.IntType}

	for i := int64(0); i < 10; i++ {
		mt.set(i, i)
	}

	buffer := new(bytes.Buffer)

	if _, err := mt.write(buffer); err != nil {
		t.Fatal(err)
	}

	t.Log(buffer.Bytes())

	newt, err := read(buffer)
	if err != nil {
		t.Fatal(err)
	}

	if newt.count != mt.count {
		t.Error("wrong count")
	}
}
