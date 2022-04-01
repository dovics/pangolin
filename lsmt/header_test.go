package lsmt

import (
	"os"
	"path"
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

func TestWriteHeader(t *testing.T) {
	s, err := newTestStorage()
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := os.RemoveAll(testOption.WorkDir); err != nil {
			t.Fatal(err)
		}
	}()

	file, err := os.Create(path.Join(testOption.WorkDir, "temp"))
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := file.Close(); err != nil {
			t.Fatal(err)
		}
	}()

	s.Insert(&db.Entry{Key: 1, Value: 1, Type: db.IntType, Tags: []string{"test"}})

	if err := s.WriteHeader(file, []uint32{100}, []uint32{100}); err != nil {
		t.Fatal(err)
	}

}
