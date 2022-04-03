package lsmt

import (
	"os"
	"testing"
	"time"

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

	s := &Storage{
		option: testOption,
		mem:    NewMemtable(),
	}

	return s, nil
}

func TestMemStorage(t *testing.T) {
	s, err := newTestStorage()
	if err != nil {
		t.Fatal(err)
	}

	for i := int64(0); i < 100000; i++ {
		err := s.Insert(&db.Entry{Key: i, Value: i, Type: db.IntType, Tags: []string{"test"}})
		if err != nil {
			t.Error(err)
		}
	}

	time.Sleep(5 * time.Second)
}
