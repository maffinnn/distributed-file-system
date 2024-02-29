package service

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

type Cache struct {
	mu sync.Mutex
	cc map[string]*Entry
}

func NewCache() *Cache {
	return &Cache{
		cc: make(map[string]*Entry),
	}
}

func (c *Cache) Set(key string, value []byte) (int, error) {
	c.cc[key] = &Entry{}
	return c.cc[key].Write(value)
}

func (c *Cache) Get(key string) (*Entry, error) {
	if _, ok := c.cc[key]; !ok {
		return nil, fmt.Errorf("invalid cache key %s", key)
	}
	return c.cc[key], nil
}

func (c *Cache) Reset(key string) {}

// cache object implements one-copy semantics
type Entry struct {
	lastValidated time.Time // time when the cache entry was last validated
	b             *bytes.Buffer
}

func (e *Entry) Len() int { return e.b.Len() }

func (e *Entry) Bytes() []byte { return e.b.Bytes() }

func (e *Entry) Reset() { e.b.Reset() }

func (e *Entry) Write(b []byte) (int, error) { return e.b.Write(b) }
