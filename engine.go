package db

import (
	"sync"
)

var (
	enginesMu sync.RWMutex
	engines   = make(map[string]func() Engine)
)

func Register(name string, f func() Engine) {
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
	Key   uint64
	Value interface{}
	Size  uint64

	Tags   []string
	Lables map[string]string
}

type Engine interface {
	Insert(*Entry) error
}
