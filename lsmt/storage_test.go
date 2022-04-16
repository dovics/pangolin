package lsmt

import (
	"os"
	"reflect"
	"testing"
	"time"

	db "github.com/dovics/pangolin"
)

var testOption *Option = &Option{
	WorkDir:        "./test_lsm",
	WalPath:        "./test_wal",
	CompressEnable: true,
	MemtableSize:   1024,
	DiskfileCount:  10,

	MinioEndpoint:        "192.168.0.251:9000",
	MinioAccessKeyID:     "wangrushen",
	MinioSecretAccessKey: "wangrushen",
	MinioUseSSL:          false,
}

func newTestStorage() (*Storage, error) {
	if err := os.Mkdir(testOption.WorkDir, 0750); err != nil {
		return nil, err
	}

	wal, err := NewWAL(testOption.WalPath)
	if err != nil {
		return nil, err
	}

	mt, err := wal.Load()
	if err != nil {
		return nil, err
	}

	dt, err := NewDiskTable(testOption.WorkDir, testOption.DiskfileCount)
	if err != nil {
		return nil, err
	}

	remoteOption, err := NewRemoteOption(testOption)
	if err != nil {
		return nil, err
	}

	rt, err := NewRemoteTable(remoteOption, dt)
	if err != nil {
		return nil, err
	}

	s := &Storage{
		option: testOption,
		mem:    mt,
		disk:   dt,
		remote: rt,
	}

	return s, nil
}

func clean(s *Storage) {
	if err := s.Close(); err != nil {
		panic(err)
	}

	if err := os.RemoveAll(testOption.WorkDir); err != nil {
		panic(err)
	}
}

func TestStorage(t *testing.T) {
	s, err := newTestStorage()
	if err != nil {
		t.Fatal(err)
	}
	defer clean(s)

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

	time.Sleep(10 * time.Second)
}
