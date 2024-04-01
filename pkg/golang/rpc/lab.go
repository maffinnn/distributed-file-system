package rpc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"reflect"
)

type LabCodec struct{}

var _ Codec = (*LabCodec)(nil)

func NewLabCodec() Codec {
	return &LabCodec{}
}

func (c *LabCodec) Encode(h *Header, body interface{}) ([]byte, error) {
	hb, err := c.EncodeHeader(h)
	if err != nil {
		return nil, err
	}
	bb, err := c.EncodeBody(body)
	if err != nil {
		return nil, err
	}
	return append(hb, bb...), nil
}

func (c *LabCodec) EncodeHeader(h *Header) ([]byte, error) {
	var buf bytes.Buffer
	b := []byte(h.ServiceMethod)
	lenbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenbuf[:4], uint32(len(b)))
	buf.Write(lenbuf[:4])
	buf.Write(b)
	// fmt.Printf("serviceMethod len: %d, serviceMethod: %v\n", len(b), b)
	seqBuf := make([]byte, 8)
	binary.LittleEndian.PutUint64(seqBuf[:], h.Seq)
	buf.Write(seqBuf)
	// fmt.Printf("seq: %v\n", seqBuf)
	b = []byte(h.Error)
	lenbuf = make([]byte, 4)
	binary.LittleEndian.PutUint32(lenbuf[:4], uint32(len(b)))
	buf.Write(lenbuf[:4])
	buf.Write(b)
	// fmt.Printf("error len: %d, error: %v\n", len(b), b)
	totalHeaderLen := uint32(buf.Len())
	lenbuf = make([]byte, 4)
	binary.LittleEndian.PutUint32(lenbuf[:4], totalHeaderLen)
	return append(lenbuf, buf.Bytes()...), nil
}

func (c *LabCodec) EncodeBody(body interface{}) ([]byte, error) {
	if reflect.TypeOf(body).Kind() == reflect.Ptr {
		body = reflect.ValueOf(body).Elem().Interface()
	}
	var fieldBuf, buf bytes.Buffer
	typeName := []byte(reflect.TypeOf(body).Name())
	typeNameLenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(typeNameLenBuf[:4], uint32(len(typeName)))
	buf.Write(typeNameLenBuf[:4])
	buf.Write(typeName)
	// fmt.Printf("typeNameLen %v, typeName: %v\n", len(typeName), typeName)
	numOfFields := reflect.TypeOf(body).NumField()
	for i := 0; i < numOfFields; i++ {
		f := reflect.TypeOf(body).Field(i)
		fieldName := []byte(f.Name)
		fieldNameLenBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(fieldNameLenBuf[:4], uint32(len(fieldName)))
		fieldBuf.Write(fieldNameLenBuf[:4])
		fieldBuf.Write(fieldName)
		// fmt.Printf("field name %v, in bytes %v\n", f.Name, fieldName)
		fieldValue := reflect.ValueOf(body).FieldByName(f.Name).Interface()
		switch fieldValue.(type) {
		case string:
			fieldTypeName := []byte(f.Type.Name())
			fieldTypeNameLenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(fieldTypeNameLenBuf, uint32(len(fieldTypeName)))
			// fmt.Printf("field type name %v, in bytes %v\n", f.Type.Name(), fieldTypeName)
			fieldBuf.Write(fieldTypeNameLenBuf)
			fieldBuf.Write(fieldTypeName)
			encodeString(&fieldBuf, fieldValue.(string))
		case bool:
			fieldTypeName := []byte(f.Type.Name())
			fieldTypeNameLenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(fieldTypeNameLenBuf, uint32(len(fieldTypeName)))
			fieldBuf.Write(fieldTypeNameLenBuf)
			fieldBuf.Write(fieldTypeName)
			encodeBool(&fieldBuf, fieldValue.(bool))
		case int64:
			fieldTypeName := []byte(f.Type.Name())
			fieldTypeNameLenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(fieldTypeNameLenBuf, uint32(len(fieldTypeName)))
			fieldBuf.Write(fieldTypeNameLenBuf)
			fieldBuf.Write(fieldTypeName)
			encodeInt64(&fieldBuf, fieldValue.(int64))
		case []byte:
			fieldTypeName := []byte("[]byte")
			fieldTypeNameLenBuf := make([]byte, 4)
			binary.LittleEndian.PutUint32(fieldTypeNameLenBuf, uint32(len(fieldTypeName)))
			fieldBuf.Write(fieldTypeNameLenBuf)
			fieldBuf.Write(fieldTypeName)
			encodeByteSlice(&fieldBuf, fieldValue.([]byte))
		default:
			return nil, fmt.Errorf("unsupported data type")
		}
	}
	totalFieldLenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(totalFieldLenBuf, uint32(fieldBuf.Len()))
	buf.Write(totalFieldLenBuf[:4])
	buf.Write(fieldBuf.Bytes())
	totalLenBuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(totalLenBuf, uint32(buf.Len()))
	return append(totalLenBuf[:4], buf.Bytes()...), nil
}

func (c *LabCodec) Decode(data []byte, m *Message) error {
	// log.Printf("custom types: %v", customTypes)
	headerLen := binary.LittleEndian.Uint32(data[:4])
	var base uint32 = 4
	h, err := c.DecodeHeader(data[base : base+headerLen])
	if err != nil {
		return fmt.Errorf("error decoding header: %v", err)
	}
	m.Header = h
	base += headerLen
	bodyLen := binary.LittleEndian.Uint32(data[base : base+4])
	base += 4
	o, err := c.DecodeBody(data[base : base+bodyLen])
	m.Body = o
	return err
}

func (c *LabCodec) DecodeHeader(data []byte) (Header, error) {
	var h Header
	datalen := binary.LittleEndian.Uint32(data[:4])
	var base uint32 = 4
	h.ServiceMethod = string(bytes.Trim(data[base:base+datalen], "\x00"))
	base += datalen
	h.Seq = binary.LittleEndian.Uint64(data[base : base+8])
	base += 8
	datalen = binary.LittleEndian.Uint32(data[base : base+4])
	base += 4
	h.Error = string(bytes.Trim(data[base:base+datalen], "\x00"))
	return h, nil
}

func (c *LabCodec) DecodeBody(data []byte) (interface{}, error) {
	var base uint32 = 0
	dataLen := binary.LittleEndian.Uint32(data[base : base+4])
	base += 4
	typeOfObject := string(bytes.Trim(data[base:base+dataLen], "\x00"))
	base += dataLen
	o := findCustomType(typeOfObject)
	if o == nil {
		return nil, fmt.Errorf("unable to find custom type %s", typeOfObject)
	}
	totalFieldLen := binary.LittleEndian.Uint32(data[base : base+4])
	base += 4
	end := base + totalFieldLen
	newO := reflect.New(reflect.TypeOf(o))
	for base < end {
		fieldNameLen := binary.LittleEndian.Uint32(data[base : base+4])
		// fmt.Printf("fieldNameLen %d \n", fieldNameLen)
		base += 4
		fieldName := string(bytes.Trim(data[base:base+fieldNameLen], "\x00"))
		base += fieldNameLen
		fieldTypeNameLen := binary.LittleEndian.Uint32(data[base : base+4])
		base += 4
		fieldType := string(bytes.Trim(data[base:base+fieldTypeNameLen], "\x00"))
		base += fieldTypeNameLen
		fieldValueLen := binary.LittleEndian.Uint32(data[base : base+4])
		// fmt.Printf("field name: %v, field type: %v, field value len: %v\n", fieldName, fieldType, fieldValueLen)
		base += 4
		switch fieldType {
		case "string":
			b := bytes.Trim(data[base:base+fieldValueLen], "\x00")
			newO.Elem().FieldByName(fieldName).SetString(string(b))
		case "bool":
			b := uint8(data[base])
			newO.Elem().FieldByName(fieldName).SetBool(b == 1)
		case "int64":
			b := binary.LittleEndian.Uint64(data[base : base+fieldValueLen])
			newO.Elem().FieldByName(fieldName).SetInt(int64(b))
		case "[]byte":
			newO.Elem().FieldByName(fieldName).SetBytes(data[base : base+fieldValueLen])
		default:
			return nil, fmt.Errorf("unsupported data type")
		}
		base += fieldValueLen
	}
	return newO.Interface(), nil
}

func encodeString(w io.Writer, s string) {
	buf := []byte(s)
	lenbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenbuf, uint32(len(buf)))
	w.Write(lenbuf[:4])
	w.Write(buf)
}

func encodeInt64(w io.Writer, i int64) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, i); err != nil {
		log.Println("rpc codec: corba error encoding message:", err)
		return
	}
	lenbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenbuf, uint32(buf.Len()))
	w.Write(lenbuf[:4])
	w.Write(buf.Bytes())
}

func encodeBool(w io.Writer, b bool) {
	var buf bytes.Buffer
	if err := binary.Write(&buf, binary.LittleEndian, b); err != nil {
		log.Println("rpc codec: corba error encoding message:", err)
		return
	}
	lenbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenbuf, uint32(buf.Len()))
	w.Write(lenbuf[:4])
	w.Write(buf.Bytes())
}

func encodeByteSlice(w io.Writer, b []byte) {
	lenbuf := make([]byte, 4)
	binary.LittleEndian.PutUint32(lenbuf, uint32(len(b)))
	w.Write(lenbuf[:4])
	w.Write(b)
}

func findCustomType(typeName string) interface{} {
	if _, ok := customTypes[typeName]; !ok {
		return nil
	}
	return customTypes[typeName]
}
