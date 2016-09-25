package avro

import (
	"encoding/binary"
	"fmt"
	"io"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

func zencode(v int) uint64 {
	//return uint64((v >> 63) ^ (v << 1))
	if v >= 0 {
		return uint64(v) << 1
	} else {
		return (uint64(-v) << 1) - 1
	}
}

func zdecode(v uint64) int {
	//return int((v >> 1) ^ -(v & 1))
	if (v & 1) == 0 {
		return int(v >> 1)
	} else {
		return -int((v + 1) >> 1)
	}
}

type ValueError struct {
	Value        interface{}
	ExpectedType string
}

func (err ValueError) Error() string {
	return fmt.Sprintf("ValueError. Expect %s, found %v of type %T", err.ExpectedType, err.Value, err.Value)
}

type Reader interface {
	io.Reader
	io.ByteReader
}

type Schema interface {
	Encode(w io.Writer, v interface{})
	Decode(r Reader) interface{}
	String() string
	SchemaName() string
}

type RecordField struct {
	Name   string
	Schema Schema
}

type Record struct {
	Schema Schema
	Values []interface{}
}

func encodeVarInt(w io.Writer, v int) {
	var buf [10]byte
	l := binary.PutUvarint(buf[:], uint64(zencode(v)))
	_, err := w.Write(buf[:l])
	check(err)
}

func decodeVarInt(r Reader) int {
	v, err := binary.ReadUvarint(r)
	check(err)
	return zdecode(v)
}

func SchemaName(v interface{}) string {
	switch t := v.(type) {
	case int32:
		return "int"
	case int:
		return "long"
	case []byte:
		return "bytes"
	case string:
		return "string"
	case float64:
		return "double"
	case Record:
		return t.Schema.SchemaName()
	case []interface{}:
		return "[]" + SchemaName(t[0])
	}
	panic("Unserializable!")
	return ""
}
