/*
Encode and decode to/from Avro serialization format.
*/
package avro

import (
	"fmt"
	"io"
)

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

type SchemaRepo interface {
	Append(j string) Schema
	AppendSchema(name string, schema Schema)
	Get(name string) Schema
}

type RecordField struct {
	Name   string
	Schema Schema
}

type Record struct {
	Schema Schema
	Values []interface{}
}

func Encode(w io.Writer, schema Schema, v interface{}) (err error) {
	defer func() {
		err = recover().(error)
	}()
	schema.Encode(w, v)
	return
}

func Decode(r Reader, schema Schema) (v interface{}, err error) {
	defer func() {
		err = recover().(error)
	}()
	v = schema.Decode(r)
	return
}

func SchemaName(v interface{}) string {
	switch t := v.(type) {
	case nil:
		return "null"
	case bool:
		return "boolean"
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
