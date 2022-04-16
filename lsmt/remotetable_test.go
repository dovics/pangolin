package lsmt

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"reflect"
	"strconv"
	"testing"
	"time"

	db "github.com/dovics/pangolin"
)

func (rt *remotetable) prepare(start, end int64) {
	if _, err := os.Stat(rt.option.WorkDir); os.IsNotExist(err) {
		if err := os.Mkdir(rt.option.WorkDir, 0750); err != nil {
			panic(err)
		}
	}

	rt.dt.prepare(start, end)

	if err := rt.upload(path.Join(rt.option.WorkDir, fmt.Sprintf("%d-%d", start, end))); err != nil {
		panic(err)
	}
}

func TestNewRemoteTable(t *testing.T) {
	remoteOption, err := NewRemoteOption(testOption)
	if err != nil {
		t.Fatal(err)
	}

	rt, err := NewRemoteTable(remoteOption, nil)
	if err != nil {
		t.Fatal(err)
	}

	if err := rt.client.RemoveBucket(context.Background(), rt.option.BucketName); err != nil {
		t.Fatal(err)
	}
}

func TestUploadAndDownload(t *testing.T) {
	remoteOption, err := NewRemoteOption(testOption)
	if err != nil {
		t.Fatal(err)
	}

	rt, err := NewRemoteTable(remoteOption, nil)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := rt.client.RemoveBucketReplication(context.Background(), rt.option.BucketName); err != nil {
			t.Fatal(err)
		}
	}()

	tempFileName := "test_temp"
	file, err := os.Create(tempFileName)
	if err != nil {
		t.Fatal(err)
	}

	fileContent := []byte("hello world")
	if _, err := file.Write(fileContent); err != nil {
		t.Fatal(err)
	}

	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := os.Remove(tempFileName); err != nil {
			t.Fatal(err)
		}
	}()

	if err := rt.upload(tempFileName); err != nil {
		t.Fatal(err)
	}

	if err := rt.download(tempFileName); err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := os.RemoveAll(rt.option.WorkDir); err != nil {
			t.Fatal(err)
		}
	}()

	buffer, err := ioutil.ReadFile(path.Join(rt.option.WorkDir, tempFileName))
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(buffer, fileContent) {
		t.Error(err)
	}
}

func TestGetRange(t *testing.T) {
	os.Mkdir(testOption.WorkDir, 0750)
	defer os.RemoveAll(testOption.WorkDir)
	dt, err := NewDiskTable(testOption.WorkDir, testOption.DiskfileCount)
	if err != nil {
		panic(err)
	}

	remoteOption, err := NewRemoteOption(testOption)
	if err != nil {
		t.Fatal(err)
	}

	rt, err := NewRemoteTable(remoteOption, dt)
	if err != nil {
		t.Fatal(err)
	}

	defer func() {
		if err := rt.client.RemoveBucketReplication(context.Background(), rt.option.BucketName); err != nil {
			t.Fatal(err)
		}
	}()

	rt.prepare(0, 1000)

	result, err := rt.getRange(20, 40, nil)
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

func TestParseObjectKey(t *testing.T) {
	start, end := time.Now().UnixMicro(), time.Now().UnixMicro()
	key := strconv.FormatInt(start, 10) + "-" + strconv.FormatInt(end, 10)
	file, err := parseObjectKey(key)
	if err != nil {
		t.Fatal(err)
	}

	if file.end != end || file.start != start {
		t.Errorf("wrong parse result, expect: start(%d) end(%d), got start(%d) end(%d)\n", start, end, file.end, file.start)
	}
}
