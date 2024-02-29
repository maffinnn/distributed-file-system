package service

import (
	"bytes"
	"time"
)

// cache object implements one-copy semantics
type Entry struct {
	lastValidated time.Time // time when the cache entry was last validated
	b             *bytes.Buffer
}

func (e *Entry) Len() int { return e.b.Len() }

func (e *Entry) Bytes() []byte { return e.b.Bytes() }

func (e *Entry) Reset() { e.b.Reset() }

func (e *Entry) Write(b []byte) (int, error) { return e.b.Write(b) }
