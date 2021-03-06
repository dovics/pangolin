package blot

import (
	"encoding/json"
	"reflect"
	"testing"

	db "github.com/dovics/pangolin"
	"go.etcd.io/bbolt"
)

var testOption *Option = &Option{
	Path: "./test_bblot",
}

func newTestStorage() (*Storage, error) {
	bblotDB, err := bbolt.Open(testOption.Path, 0666, nil)
	if err != nil {
		return nil, err
	}

	return &Storage{
		option:        testOption,
		db:            bblotDB,
		unmarshalFunc: make(map[string]UnmarshalFunc),
	}, nil
}

func TestStorage(t *testing.T) {
	s, err := newTestStorage()
	if err != nil {
		t.Fatal(err)
	}

	s.SetUnmarshalFunc("test", func(value []byte) (interface{}, error) {
		var v int64
		if err := json.Unmarshal(value, &v); err != nil {
			return nil, err
		}
		return v, nil
	})

	for i := int64(0); i < 1000; i++ {
		err := s.Insert(&db.Entry{KV: db.KV{Key: i, Value: i}, Type: db.IntType, Tags: []string{"test"}})
		if err != nil {
			t.Fatal(err)
		}
	}

	result, err := s.GetRange(20, 40, nil)
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
