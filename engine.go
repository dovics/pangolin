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

type Engine interface {
	Insert(*Entry) error
	GetRange(startTime, endTime int64, filter *QueryFilter) ([]interface{}, error)
}
