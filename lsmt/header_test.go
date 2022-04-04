package lsmt

import (
	"bytes"
	"reflect"
	"strconv"
	"testing"

	db "github.com/dovics/pangolin"
)

func TestHeader(t *testing.T) {
	buffer := new(bytes.Buffer)

	indexes := []*index{}
	expectResult := [db.TypeCount]map[string]*index{
		make(map[string]*index),
		make(map[string]*index),
		make(map[string]*index),
		make(map[string]*index),
	}

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

	for _, index := range indexes {
		expectResult[index.t][index.index] = index
	}

	if err := writeHeader(buffer, indexes); err != nil {
		t.Fatal(err)
	}

	t.Log(buffer.Bytes())

	result, err := readHeader(buffer)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(expectResult, result) {
		t.Errorf("expect equal, expectResult: %v, result: %v", indexes, result)
	}

	t.Log(result)
}
