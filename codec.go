package avro

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
)

type NullSchema struct{}

var nullSchema NullSchema

func (NullSchema) Encode(w io.Writer, v interface{}) {
	return
}

func (NullSchema) Decode(r Reader) interface{} {
	return nil
}

func (NullSchema) String() string {
	return "NullSchema"
}

func (NullSchema) SchemaName() string {
	return "null"
}

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

func (schema ArraySchema) String() string {
	return fmt.Sprintf("ArrayCodec<%s>", schema.ItemSchema)
}

func (schema ArraySchema) Encode(w io.Writer, v interface{}) {
	arr := v.([]interface{})
	encodeVarInt(w, len(arr))
	for _, item := range arr {
		schema.ItemSchema.Encode(w, item)
	}
	_, err := w.Write([]byte{0})
	check(err)
}

func (schema ArraySchema) Decode(r Reader) interface{} {
	arrLen := decodeVarInt(r)
	buf := make([]interface{}, arrLen)
	for i := range buf {
		buf[i] = schema.ItemSchema.Decode(r)
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

func (schema RecordSchema) Encode(w io.Writer, v interface{}) {
	rec := v.(Record)
	if len(rec.Values) != len(schema.Fields) {
		panic(errors.New(fmt.Sprintf("Record length mismatch. Provided: %d, expected: %d", len(rec.Values), len(schema.Fields))))
	}
	for i, item := range rec.Values {
		schema.Fields[i].Schema.Encode(w, item)
	}
}

func (schema RecordSchema) Decode(r Reader) interface{} {
	rec := Record{Schema: schema, Values: make([]interface{}, len(schema.Fields))}
	for i, c := range schema.Fields {
		rec.Values[i] = c.Schema.Decode(r)
	}
	return rec
}

func (schema RecordSchema) String() string {
	var fields []string
	for _, f := range schema.Fields {
		fields = append(fields, fmt.Sprintf("%s: %s", f.Name, f.Schema.String()))
	}
	return fmt.Sprintf("%s<%s>", schema.Name, strings.Join(fields, ","))
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

func (schema UnionSchema) Encode(w io.Writer, v interface{}) {
	index, option := schema.getOptionForValue(v)
	encodeVarInt(w, index)
	option.Encode(w, v)
}

func (schema UnionSchema) Decode(r Reader) interface{} {
	var buf [1]byte
	_, err := r.Read(buf[:])
	check(err)
	return schema.Options[buf[0]].Decode(r)
}

// inline union have no explicit schema name
func (schema UnionSchema) SchemaName() string {
	return "union"
}
