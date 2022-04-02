package lsmt

import (
	"bytes"
	"os"
	"reflect"
	"strconv"
	"testing"

	"github.com/dovics/db"
)

var testOption *Option = &Option{
	WorkDir:            "./lsm",
	CompressEnable:     true,
	MaxMemtableSize:    1024,
	MaxCompactFileSize: 1024,
}

func newTestStorage() (*Storage, error) {
	if err := os.Mkdir(testOption.WorkDir, 0750); err != nil {
		return nil, err
	}

	s := &Storage{
		option: testOption,
	}

	for i := 0; i < int(db.TypeCount); i++ {
		s.memtables[i] = make(map[string]*memtable)
	}

	return s, nil
}

func TestHeader(t *testing.T) {
	buffer := new(bytes.Buffer)

	indexes := []*Index{}
	for i := 0; i < 100; i++ {
		indexes = append(indexes, &Index{
			index:  "test" + strconv.Itoa(i),
			t:      db.IntType,
			count:  10,
			max:    uint32(10 * (i + 1)),
			min:    uint32(10 * i),
			offset: uint32(100 * i),
			length: 100,
		})
	}

	if err := WriteHeader(buffer, indexes); err != nil {
		t.Fatal(err)
	}

	t.Log(buffer.Bytes())

	newIndexes, err := ReadHeader(buffer)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(indexes, newIndexes) {
		t.Errorf("expect equal, index length: %v, newIndexes length: %v", len(indexes), len(newIndexes))
	}

}
