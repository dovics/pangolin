package lsmt

import (
	"fmt"
	"os"
	"path"
	"reflect"
	"testing"

	db "github.com/dovics/pangolin"
)

func (dt *disktable) prepare(start, end int64) {
	mt := NewMemtable()

	for i := start; i < end; i++ {
		mt.insert(&db.Entry{KV: db.KV{Key: i, Value: i}, Type: db.IntType, Tags: []string{"test"}})
	}

	filePath := path.Join(dt.workDir, fmt.Sprintf("%d-%d", start, end))
	file, err := os.Create(filePath)
	if err != nil {
		panic(err)
	}

	defer file.Close()
	if err := mt.write(file); err != nil {
		panic(err)
	}

	if err := dt.AddFile(filePath); err != nil {
		panic(err)
	}

}

func TestDiskTable(t *testing.T) {
	os.Mkdir(testOption.WorkDir, 0750)
	defer os.RemoveAll(testOption.WorkDir)
	dt, err := NewDiskTable(testOption.WorkDir, testOption.DiskfileCount)
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

	dt.prepare(0, 1000)

	result, err := dt.getRange(20, 40, nil)
	if err != nil {
		t.Fatal(err)
	}

	expectResult := make([]db.KV, 20)
	for i := 20; i < 40; i++ {
		expectResult[i-20] = db.KV{Key: int64(i), Value: int64(i)}
	}

	if !reflect.DeepEqual(expectResult, result) {
		t.Errorf("expect %v, got %v\n", expectResult, result)
	}
}

func TestAddFile(t *testing.T) {
	os.Mkdir(testOption.WorkDir, 0750)
	defer os.RemoveAll(testOption.WorkDir)
	dt, err := NewDiskTable(testOption.WorkDir, testOption.DiskfileCount)
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

	if err := dt.AddFile("test_lsm/0-239"); err != nil {
		t.Fatal(err)
	}

	if len(dt.files) != 1 {
		t.Error("wrong files count")
	}
}

func TestLRUCache(t *testing.T) {
	os.Mkdir(testOption.WorkDir, 0750)
	defer os.RemoveAll(testOption.WorkDir)
	dt, err := NewDiskTable(testOption.WorkDir, testOption.DiskfileCount)
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

	for i := 0; i < 100; i++ {
		if err := dt.AddFile(fmt.Sprintf(path.Join(testOption.WorkDir, "%d-%d"), i*100, (i+1)*100)); err != nil {
			t.Fatal(err)
		}
	}

	if len(dt.files) != 10 {
		t.Errorf("wrong files count: %d", len(dt.files))
	}

	for i, file := range dt.files {
		expectPath := fmt.Sprintf(path.Join(testOption.WorkDir, "%d-%d"), (i+90)*100, (i+91)*100)
		if file.path != expectPath {
			t.Errorf("wrong file name: expect: %s, got: %s", file.path, file.path)
		}
	}
}
