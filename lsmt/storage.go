package lsmt

import (
	"errors"
	"log"
	"os"
	"path"
	"strconv"
	"sync/atomic"

	db "github.com/dovics/pangolin"
	"github.com/dovics/pangolin/compress"
	"github.com/google/uuid"
)

func init() {
	db.Register("lsm", func(uuid uuid.UUID, o interface{}) (db.Engine, error) {
		if o == nil {
			o = DefaultOption
		}

		option, ok := o.(*Option)
		if !ok {
			return nil, errors.New("wrong option type")
		}

		option.uuid = uuid

		if _, err := os.Stat(option.WorkDir); os.IsNotExist(err) {
			if err := os.Mkdir(option.WorkDir, 0750); err != nil {
				return nil, err
			}
		}

		wal, err := NewWAL(option.WalPath)
		if err != nil {
			return nil, err
		}

		mt, err := wal.Load()
		if err != nil {
			return nil, err
		}

		dt, err := NewDiskTable(option.WorkDir, option.DiskfileCount)
		if err != nil {
			return nil, err
		}

		remoteOption, err := NewRemoteOption(option)
		if err != nil {
			return nil, err
		}

		rt, err := NewRemoteTable(remoteOption, dt)
		if err != nil {
			return nil, err
		}

		s := &Storage{
			option: option,
			mem:    mt,
			disk:   dt,
			remote: rt,
		}

		return s, nil
	})
}

type Option struct {
	uuid           uuid.UUID
	WorkDir        string
	WalPath        string
	CompressEnable bool
	MemtableSize   uint64
	DiskfileCount  int

	MinioEndpoint        string
	MinioAccessKeyID     string
	MinioSecretAccessKey string
	MinioUseSSL          bool
}

var DefaultOption *Option = &Option{
	WorkDir:        "./lsm",
	WalPath:        "./wal",
	CompressEnable: true,
	MemtableSize:   1024 * 1024,
	DiskfileCount:  10,

	MinioEndpoint:        "192.168.0.251:9000",
	MinioAccessKeyID:     "wangrushen",
	MinioSecretAccessKey: "wangrushen",
	MinioUseSSL:          false,
}

type Storage struct {
	option *Option

	isFlashing int32
	flashTable *memtable

	mem    *memtable
	disk   *disktable
	remote *remotetable
}

func (s *Storage) Close() error {
	return s.disk.Close()
}

func (s *Storage) Insert(e *db.Entry) error {
	if err := s.mem.insert(e); err != nil {
		return err
	}

	go s.saveToFileIfNeeded()
	return nil
}

func (s *Storage) GetRange(startTime, endTime int64, filter *db.QueryFilter) ([]interface{}, error) {
	result := []interface{}{}

	memResult, err := s.mem.getRange(startTime, endTime, filter)
	if err != nil {
		return nil, err
	}

	result = append(result, memResult...)

	if s.flashTable != nil {
		flashResult, err := s.flashTable.getRange(startTime, endTime, filter)
		if err != nil {
			return nil, err
		}

		result = append(result, flashResult...)
	}

	diskResult, err := s.disk.getRange(startTime, endTime, filter)
	if err != nil {
		return nil, err
	}

	result = append(result, diskResult...)

	return result, nil
}

func (s *Storage) saveToFileIfNeeded() {
	if s.mem.size < s.option.MemtableSize {
		return
	}

	if !atomic.CompareAndSwapInt32(&s.isFlashing, 0, 1) {
		return
	}

	s.flashTable, s.mem = s.mem, NewMemtable()

	filePath := path.Join(s.option.WorkDir,
		strconv.FormatInt(s.flashTable.minKey, 10)+"-"+strconv.FormatInt(s.flashTable.maxKey, 10))
	log.Printf("save %s\n", filePath)

	file, err := os.Create(filePath)
	if err != nil {
		log.Println("create file error: ", err)
		return
	}

	if err := s.flashTable.write(file); err != nil {
		log.Println("memtable write error: ", err)
		file.Close()
		return
	}

	if err := file.Sync(); err != nil {
		log.Println("file sync error: ", err)
		file.Close()
		return
	}

	if err := s.disk.AddFile(filePath); err != nil {
		log.Println("file add error: ", err)
		file.Close()
		return
	}

	if err := file.Close(); err != nil {
		log.Println("file close error: ", err)
		return
	}

	if !atomic.CompareAndSwapInt32(&s.isFlashing, 1, 0) {
		return
	}

	if err := s.remote.upload(filePath); err != nil {
		log.Println("file upload error: ", err)
		return
	}
}

type Encoder interface {
	Write(interface{}) error
	Bytes() ([]byte, error)
	Flush()
	Reset()
}

func NewEncoder(t db.ValueType, size int) Encoder {
	switch t {
	case db.IntType:
		return compress.NewIntegerEncoder(size)
	case db.FloatType:
		return compress.NewFloatEncoder()
	case db.StringType:
		return compress.NewStringEncoder(size)
	default:
		return compress.NewStringEncoder(size)
	}
}

type Decoder interface {
	SetBytes(b []byte) error
	Next() bool
	Read() interface{}
	Error() error
}

func NewDecoder(t db.ValueType) Decoder {
	switch t {
	case db.IntType:
		return &compress.IntegerDecoder{}
	case db.FloatType:
		return &compress.FloatDecoder{}
	case db.StringType:
		return &compress.StringDecoder{}
	default:
		return &compress.StringDecoder{}
	}
}
