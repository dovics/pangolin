package pangolin

import (
	"errors"
	"reflect"

	"github.com/google/uuid"
)

type Option struct {
	UUID   string
	Engine string

	EngineOption interface{}
}

func DefaultOption(uuid string) *Option {
	return &Option{
		UUID:   uuid,
		Engine: "lsm",
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

	engine, err := engines[option.Engine](UUID, option.EngineOption)
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
	var t ValueType
	switch reflect.TypeOf(value).Kind() {
	case reflect.Int, reflect.Int64:
		t = IntType
	case reflect.Float32, reflect.Float64:
		t = FloatType
	default:
		t = StringType
	}

	return db.engine.Insert(&Entry{KV: KV{Key: time, Value: value}, Type: t})
}

func (db *DB) InsertEntry(e *Entry) error {
	if e.Value == nil {
		return errors.New("value can't be nil")
	}

	return db.engine.Insert(e)
}

func (db *DB) GetRange(startTime, endTime int64, filter *QueryFilter) ([]KV, error) {
	return db.engine.GetRange(startTime, endTime, filter)
}

func (db *DB) Get(key int64, filter *QueryFilter) (interface{}, error) {
	result, err := db.engine.GetRange(key, key+1, filter)
	if err != nil {
		return nil, err
	}

	return result[0], nil
}

func (db *DB) Engine() Engine {
	return db.engine
}

func (db *DB) Close() error {
	return db.engine.Close()
}
