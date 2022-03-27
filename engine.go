package db

import (
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
	Key   int64
	Value interface{}
	Size  uint64

	Tags   []string
	Lables map[string]string
}

type Engine interface {
	Insert(*Entry) error
}
