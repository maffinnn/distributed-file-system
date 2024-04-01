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
	if _, ok := c.cc[key]; !ok {
		c.cc[key] = NewEntry()
	} else {
		c.cc[key].Reset()
	}
	return c.cc[key].Write(value)
}

func (c *Cache) Get(key string) (*Entry, error) {
	if _, ok := c.cc[key]; !ok {
		return nil, fmt.Errorf("invalid cache key %s", key)
	}
	return c.cc[key], nil
}

func (c *Cache) Reset(key string) {}

type Entry struct {
	dirty         bool
	lastValidated time.Time // time when the cache entry was last validated
	b             *bytes.Buffer
}

func NewEntry() *Entry {
	return &Entry{
		dirty:         false,
		lastValidated: time.Now(),
		b:             &bytes.Buffer{},
	}
}

func (e *Entry) Len() int { return e.b.Len() }

func (e *Entry) Bytes() []byte { return e.b.Bytes() }

func (e *Entry) Reset() {
	e.b.Reset()
}

func (e *Entry) Write(b []byte) (int, error) {
	return e.b.Write(b)
}
