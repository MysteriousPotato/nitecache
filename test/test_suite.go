package test

import (
	"strconv"
	"sync"
)

var mu = sync.Mutex{}
var i = 0
var start = 8000

type cache interface {
	TearDown() error
}

func GetUniqueAddr() string {
	mu.Lock()
	defer mu.Unlock()
	i++
	return "localhost:" + strconv.Itoa(start+i)
}

func TearDown(c cache) {
	go func() { _ = c.TearDown() }()
}
