package lsmt

import (
	"os"
	"path"
	"reflect"
	"testing"

	"github.com/dovics/db"
)

func prepare() {
	mt := NewMemtable()

	for i := int64(0); i < 1000; i++ {
		mt.insert(&db.Entry{Key: i, Value: i, Type: db.IntType, Tags: []string{"test"}})
	}

	if err := os.Mkdir(testOption.WorkDir, 0750); err != nil {
		panic(err)
	}

	file, err := os.Create(path.Join(testOption.WorkDir, "0-1000"))
	if err != nil {
		panic(err)
	}
	defer file.Close()
	if err := mt.write(file); err != nil {
		panic(err)
	}
}

func TestDiskTable(t *testing.T) {
	prepare()

	dt, err := NewDiskTable(testOption.WorkDir)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := dt.Close(); err != nil {
			t.Fatal(err)
		}

		if err := os.RemoveAll(testOption.WorkDir); err != nil {
			t.Fatal(err)
		}
	}()

	result, err := dt.getRange(20, 40, nil)
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
