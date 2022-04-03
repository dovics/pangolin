package lsmt

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"

	"github.com/dovics/db"
)

func TestHeader(t *testing.T) {
	buffer := new(bytes.Buffer)

	indexes := []*index{}
	for i := 0; i < 100; i++ {
		indexes = append(indexes, &index{
			index:  "test" + strconv.Itoa(i),
			t:      db.IntType,
			count:  10,
			max:    uint32(10 * (i + 1)),
			min:    uint32(10 * i),
			offset: uint32(100 * i),
			length: 100,
		})
	}

	if err := writeHeader(buffer, indexes); err != nil {
		t.Fatal(err)
	}

	t.Log(buffer.Bytes())

	newIndexes, err := readHeader(buffer)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(indexes, newIndexes) {
		t.Errorf("expect equal, index length: %v, newIndexes length: %v", len(indexes), len(newIndexes))
	}

}
