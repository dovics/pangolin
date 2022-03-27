package db

import (
	"encoding/json"
	"sync"
)

var (
	enginesMu sync.RWMutex
	engines   = make(map[string]NewEngineFunc)
)

type NewEngineFunc func(option interface{}) (Engine, error)

func Register(name string, f NewEngineFunc) {
	enginesMu.Lock()
	defer enginesMu.Unlock()
	if f == nil {
		panic("register engine is nil")
	}

	if _, dup := engines[name]; dup {
		panic("register called twice for engine " + name)
	}

	engines[name] = f
}

type Entry struct {
	Key    int64
	Value  interface{}
	Type   ValueType
	Tags   []string
	Lables map[string]string
}

type ValueType int

const (
	UnknownType ValueType = iota
	IntType
	FloatType
	StringType

	TypeCount
)

func (e *Entry) Size() uint64 {
	switch e.Type {
	case IntType, FloatType:
		return 8
	case StringType:
		return uint64(len(e.Value.(string)) * 8)
	default:
		data, _ := json.Marshal(e.Value)
		return uint64(len(data) * 8)
	}
}

type Engine interface {
	Insert(*Entry) error
}
