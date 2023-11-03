package test

import (
	"context"
	"github.com/MysteriousPotato/nitecache"
	"strconv"
	"sync"
	"testing"
	"time"
)

var (
	mu   = sync.Mutex{}
	port = 50000
)

func GetUniqueAddr() string {
	mu.Lock()
	defer mu.Unlock()

	port++
	return "127.0.0.1:" + strconv.Itoa(port)
}

func SimpleHashFunc(key string) (int, error) {
	return strconv.Atoi(key)
}

func WaitForServer(t *testing.T, c *nitecache.Cache) {
	timeout := time.Second * 5
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	healthCheckDone := make(chan struct{})
	go func() {
		for {
			if err := c.HealthCheckPeers(ctx); err == nil {
				healthCheckDone <- struct{}{}
			}
			time.Sleep(time.Millisecond * 100)
		}
	}()

	select {
	case <-healthCheckDone:
		return
	case <-ctx.Done():
		t.Fatalf("clients health check failed after %s: %v", timeout.String(), ctx.Err())
	}
}
