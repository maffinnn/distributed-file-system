package service

import (
	"bytes"
	"fmt"
	"sync"
	"time"
)

type Cache struct {
	mu sync.Mutex
	sync.Map
}

func NewCache() *Cache {
	return &Cache{}
}

func (c *Cache) Set(key string, value []byte) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry := NewEntry()
	n, err := entry.Write(value)
	if err != nil {
		return 0, err
	}
	c.Store(key, entry)
	return n, nil
}

func (c *Cache) Get(key string) (*Entry, error) {
	v, ok := c.Load(key)
	if !ok {
		return nil, fmt.Errorf("invalid cache key %s", key)
	}
	return v.(*Entry), nil
}

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
