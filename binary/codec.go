package binary

import (
	"encoding/binary"
	"errors"
	"fmt"
	. "github.com/galtsev/avro"
	"io"
	"math"
	"strings"
)

type NullSchema struct{}

var Null NullSchema

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

var Integer IntSchema

func (IntSchema) Encode(w io.Writer, v interface{}) {
	EncodeVarInt(w, int(v.(int32)))
}

func (IntSchema) Decode(r Reader) interface{} {
	return int32(DecodeVarInt(r))
}

func (IntSchema) String() string {
	return "IntSchema"
}

func (IntSchema) SchemaName() string {
	return "int"
}

type LongSchema struct{}

var Long LongSchema

func (LongSchema) Encode(w io.Writer, v interface{}) {
	EncodeVarInt(w, v.(int))
}

func (LongSchema) Decode(r Reader) interface{} {
	return DecodeVarInt(r)
}

func (LongSchema) String() string {
	return "LongCodec"
}

func (LongSchema) SchemaName() string {
	return "long"
}

type BytesSchema struct{}

func encodeBytes(w io.Writer, buf []byte) {
	EncodeVarInt(w, len(buf))
	_, err := w.Write(buf)
	check(err)
}

func decodeBytes(r Reader) []byte {
	bufLen := DecodeVarInt(r)
	buf := make([]byte, bufLen, bufLen)
	_, err := r.Read(buf)
	check(err)
	return buf
}

var Bytes BytesSchema

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

var String StringSchema

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

var Boolean BooleanSchema

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

var Double DoubleSchema

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

type FixedSchema struct {
	Name string
	Size int
}

func (schema FixedSchema) String() string {
	return fmt.Sprintf("Fixed<%s:%d>", schema.Name, schema.Size)
}

func (schema FixedSchema) Encode(w io.Writer, v interface{}) {
	buf := v.([]byte)
	if len(buf) != schema.Size {
		panic(ValueError{Value: v, ExpectedType: fmt.Sprintf("[]bytes of length %d", schema.Size)})
	}
	_, err := w.Write(buf)
	check(err)
}

func (schema FixedSchema) Decode(r Reader) interface{} {
	buf := make([]byte, schema.Size)
	_, err := r.Read(buf)
	check(err)
	return buf
}

func (schema FixedSchema) SchemaName() string {
	return schema.Name
}

type ArraySchema struct {
	ItemSchema Schema
}

func (schema ArraySchema) String() string {
	return fmt.Sprintf("ArrayCodec<%s>", schema.ItemSchema)
}

func (schema ArraySchema) Encode(w io.Writer, v interface{}) {
	arr := v.([]interface{})
	EncodeVarInt(w, len(arr))
	for _, item := range arr {
		schema.ItemSchema.Encode(w, item)
	}
	_, err := w.Write([]byte{0})
	check(err)
}

func (schema ArraySchema) Decode(r Reader) interface{} {
	arrLen := DecodeVarInt(r)
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

type MapSchema struct {
	ValueSchema Schema
}

func (schema MapSchema) Encode(w io.Writer, v interface{}) {
	m := v.(map[string]interface{})
	EncodeVarInt(w, len(m))
	for key, val := range m {
		String.Encode(w, key)
		schema.ValueSchema.Encode(w, val)
	}
	EncodeVarInt(w, 0)
}

func (schema MapSchema) Decode(r Reader) interface{} {
	mapLen := DecodeVarInt(r)
	res := make(map[string]interface{})
	for i := 0; i < mapLen; i++ {
		key := String.Decode(r).(string)
		value := schema.ValueSchema.Decode(r)
		res[key] = value
	}
	_ = DecodeVarInt(r)
	return res
}

func (schema MapSchema) String() string {
	return fmt.Sprintf("MapSchema<%s>", schema.ValueSchema.SchemaName())
}

// TODO:
func (schema MapSchema) SchemaName() string {
	return "map"
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
	panic(ValueError{Value: v, ExpectedType: schema.String()})
}

func (schema UnionSchema) Encode(w io.Writer, v interface{}) {
	index, option := schema.getOptionForValue(v)
	EncodeVarInt(w, index)
	option.Encode(w, v)
}

func (schema UnionSchema) Decode(r Reader) interface{} {
	ind := DecodeVarInt(r)
	return schema.Options[ind].Decode(r)
}

// inline union have no explicit schema name
func (schema UnionSchema) SchemaName() string {
	return "union"
}
