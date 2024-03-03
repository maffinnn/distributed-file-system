package rpc

import (
	"bytes"
	"encoding/gob"
	"log"
)

type GobCodec struct {
}

var _ Codec = (*GobCodec)(nil)

func NewGobCodec() Codec {
	return &GobCodec{}
}

func (c *GobCodec) Decode(data []byte, m *Message) error {
	err := gob.NewDecoder(bytes.NewReader(data)).Decode(m)
	return err
}

func (c *GobCodec) Encode(h *Header, body interface{}) ([]byte, error) {
	var m Message
	m.Header = *h
	// m.Body = body
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(m); err != nil {
		log.Println("rpc codec: gob error encoding message:", err)
		return nil, err
	}
	return buf.Bytes(), nil
}
