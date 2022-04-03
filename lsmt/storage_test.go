package lsmt

import (
	"os"
	"reflect"
	"testing"

	"github.com/dovics/db"
)

var testOption *Option = &Option{
	WorkDir:        "./lsm",
	CompressEnable: true,
	MemtableSize:   1024,
}

func newTestStorage() (*Storage, error) {
	if err := os.Mkdir(testOption.WorkDir, 0750); err != nil {
		return nil, err
	}

	dt, err := NewDiskTable(testOption.WorkDir)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		option: testOption,
		mem:    NewMemtable(),
		disk:   dt,
	}

	return s, nil
}

func TestStorage(t *testing.T) {
	s, err := newTestStorage()
	if err != nil {
		t.Fatal(err)
	}

	for i := int64(0); i < 1000; i++ {
		err := s.Insert(&db.Entry{Key: i, Value: i, Type: db.IntType, Tags: []string{"test"}})
		if err != nil {
			t.Fatal(err)
		}
	}

	result, err := s.GetRange(20, 40, nil)
	if err != nil {
		t.Fatal(err)
	}

	expectResult := make([]interface{}, 20)
	for i := 20; i < 40; i++ {
		expectResult[i-20] = int64(i)
	}

	if !reflect.DeepEqual(expectResult, result) {
		t.Errorf("expect %v, got %v\n", expectResult, result)
	}
}
