package db

import (
	"errors"
	"os"
	"unsafe"

	"github.com/google/uuid"
)

type Option struct {
	UUID    string
	WorkDir string
	engine  string

	engineOption interface{}
}

func DefaultOption(uuid string) *Option {
	return &Option{
		UUID:   uuid,
		engine: "lsm",
	}
}

type DB struct {
	Option *Option

	UUID uuid.UUID

	engine Engine
}

func OpenDB(option *Option) (*DB, error) {
	if option == nil {
		return nil, errors.New("please provide options")
	}

	UUID, err := uuid.Parse(option.UUID)
	if err != nil {
		return nil, errors.New("please provide the correct uuid")
	}

	if err := os.MkdirAll(option.WorkDir, 0750); err != nil {
		return nil, err
	}

	engine, err := engines[option.engine](option.engineOption)
	if err != nil {
		return nil, err
	}

	return &DB{
		option,
		UUID,
		engine,
	}, nil
}

func (db *DB) Insert(time int64, value interface{}) error {
	size := uint64(unsafe.Sizeof(value))
	return db.engine.Insert(&Entry{Key: time, Value: value, Size: size})
}

func (db *DB) InsertEntry(e *Entry) error {
	return db.engine.Insert(e)
}

func (db *DB) GetRange(startTime, endTime uint64) ([][]byte, error) {
	return nil, nil
}
