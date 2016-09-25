package avro

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
)

type IntSchema struct{}

var intSchema IntSchema

func (IntSchema) Encode(w io.Writer, v interface{}) {
	encodeVarInt(w, int(v.(int32)))
}

func (IntSchema) Decode(r Reader) interface{} {
	return int32(decodeVarInt(r))
}

func (IntSchema) String() string {
	return "IntSchema"
}

func (IntSchema) SchemaName() string {
	return "int"
}

type LongSchema struct{}

var longSchema LongSchema

func (LongSchema) Encode(w io.Writer, v interface{}) {
	encodeVarInt(w, v.(int))
}

func (LongSchema) Decode(r Reader) interface{} {
	return decodeVarInt(r)
}

func (LongSchema) String() string {
	return "LongCodec"
}

func (LongSchema) SchemaName() string {
	return "long"
}

type BytesSchema struct{}

func encodeBytes(w io.Writer, buf []byte) {
	encodeVarInt(w, len(buf))
	_, err := w.Write(buf)
	check(err)
}

func decodeBytes(r Reader) []byte {
	bufLen := decodeVarInt(r)
	buf := make([]byte, bufLen, bufLen)
	_, err := r.Read(buf)
	check(err)
	return buf
}

var bytesSchema BytesSchema

func (BytesSchema) Encode(w io.Writer, v interface{}) {
	encodeBytes(w, v.([]byte))
}

func (BytesSchema) Decode(r Reader) interface{} {
	return decodeBytes(r)
}

func (BytesSchema) String() string {
	return "BytesCodec"
}

func (BytesSchema) SchemaName() string {
	return "bytes"
}

type StringSchema struct{}

var stringSchema StringSchema

func (StringSchema) Encode(w io.Writer, v interface{}) {
	encodeBytes(w, []byte(v.(string)))
}

func (StringSchema) Decode(r Reader) interface{} {
	return string(decodeBytes(r))
}

func (StringSchema) String() string {
	return "StringCodec"
}

func (StringSchema) SchemaName() string {
	return "string"
}

type BooleanSchema struct{}

var booleanSchema BooleanSchema

func (BooleanSchema) String() string {
	return "BooleanCodec"
}

func (BooleanSchema) Encode(w io.Writer, v interface{}) {
	b := v.(bool)
	var buf [1]byte
	if b {
		buf[0] = 1
	}
	_, err := w.Write(buf[:])
	check(err)
}

func (BooleanSchema) Decode(r Reader) interface{} {
	var buf [1]byte
	_, err := r.Read(buf[:])
	check(err)
	return buf[0] == 1
}

func (BooleanSchema) SchemaName() string {
	return "boolean"
}

type DoubleSchema struct{}

var doubleSchema DoubleSchema

func (DoubleSchema) String() string {
	return "DoubleCodec"
}

func (DoubleSchema) Encode(w io.Writer, v interface{}) {
	var buf [8]byte
	binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v.(float64)))
	_, err := w.Write(buf[:])
	check(err)
}

func (DoubleSchema) Decode(r Reader) interface{} {
	var buf [8]byte
	_, err := r.Read(buf[:])
	check(err)
	bits := binary.LittleEndian.Uint64(buf[:])
	return math.Float64frombits(bits)
}

func (DoubleSchema) SchemaName() string {
	return "double"
}

type ArraySchema struct {
	ItemSchema Schema
}

func (codec ArraySchema) String() string {
	return fmt.Sprintf("ArrayCodec<%s>", codec.ItemSchema)
}

func (codec ArraySchema) Encode(w io.Writer, v interface{}) {
	arr := v.([]interface{})
	encodeVarInt(w, len(arr))
	for _, item := range arr {
		codec.ItemSchema.Encode(w, item)
	}
	_, err := w.Write([]byte{0})
	check(err)
}

func (codec ArraySchema) Decode(r Reader) interface{} {
	arrLen := decodeVarInt(r)
	buf := make([]interface{}, arrLen)
	for i := range buf {
		buf[i] = codec.ItemSchema.Decode(r)
	}
	//TODO: chanked arrays
	b, err := r.ReadByte()
	check(err)
	if b != byte(0) {
		panic(ValueError{b, "byte(0)"})
	}
	return buf
}

func (schema ArraySchema) SchemaName() string {
	return "[]" + schema.ItemSchema.SchemaName()
}

type RecordSchema struct {
	Name   string
	Fields []RecordField
}

func (codec RecordSchema) String() string {
	var fields []string
	for _, f := range codec.Fields {
		fields = append(fields, fmt.Sprintf("%s: %s", f.Name, f.FieldSchema.String()))
	}
	return fmt.Sprintf("%s<%s>", codec.Name, strings.Join(fields, ","))
}

func (codec RecordSchema) Encode(w io.Writer, v interface{}) {
	items := v.([]interface{})
	if len(items) != len(codec.Fields) {
		panic(errors.New(fmt.Sprintf("Record length mismatch. Provided: %d, expected: %d", len(items), len(codec.Fields))))
	}
	for i, item := range items {
		codec.Fields[i].FieldSchema.Encode(w, item)
	}
}

func (codec RecordSchema) Decode(r Reader) interface{} {
	res := make([]interface{}, len(codec.Fields))
	for i, c := range codec.Fields {
		res[i] = c.FieldSchema.Decode(r)
	}
	return res
}

func (schema RecordSchema) SchemaName() string {
	return schema.Name
}

type UnionSchema struct {
	Options []Schema
}

func (UnionSchema) String() string {
	return "UnionCodec"
}

func (schema UnionSchema) getOptionForValue(v interface{}) (index int, option Schema) {
	valueSchema := SchemaName(v)
	for index, option = range schema.Options {
		if option.SchemaName() == valueSchema {
			return
		}
	}
	return
}

func (codec UnionSchema) Encode(w io.Writer, v interface{}) {
	index, option := codec.getOptionForValue(v)
	encodeVarInt(w, index)
	option.Encode(w, v)
}

func (codec UnionSchema) Decode(r Reader) interface{} {
	var buf [1]byte
	_, err := r.Read(buf[:])
	check(err)
	return codec.Options[buf[0]].Decode(r)
}

// inline union have no explicit schema name
func (schema UnionSchema) SchemaName() string {
	return "union"
}
