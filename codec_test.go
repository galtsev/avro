package avro

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
)

var (
	longData = []struct {
		i int
		b []byte
	}{
		{0, []byte{0}},
		{1, []byte{2}},
		{2, []byte{4}},
		{-1, []byte{1}},
		{-2, []byte{3}},
		{-64, []byte{0x7F}},
		{62, []byte{124}},
		{64, []byte{0x80, 0x01}},
	}
	stringArgs = []string{
		"",
		"123",
		"abcdefgh",
		"абракадабра",
		"\n\t ,:.\x00",
		"end",
	}
	arrayData = []struct {
		a []interface{}
		b []byte
	}{
		{[]interface{}{}, []byte{0, 0}},
		{[]interface{}{0}, []byte{2, 0, 0}},
		{[]interface{}{1, -2}, []byte{4, 2, 3, 0}},
	}
)

func TestLongCodecEncode(t *testing.T) {
	for _, data := range longData {
		var w bytes.Buffer
		longSchema.Encode(&w, data.i)
		assert.Equal(t, data.b, w.Bytes())
	}
}

func TestLongCodecDecode(t *testing.T) {
	for _, data := range longData {
		buf := bytes.NewBuffer(data.b)
		v := longSchema.Decode(buf)
		assert.Equal(t, data.i, v.(int))
	}
}

func zlen(s string) []byte {
	return []byte{byte(zencode(len(s)))}
}

func TestStringCodecEncode(t *testing.T) {
	for _, s := range stringArgs {
		var w bytes.Buffer
		stringSchema.Encode(&w, s)
		expected := append(zlen(s), []byte(s)...)
		assert.Equal(t, expected, w.Bytes())
	}
}

func TestStringCodecDecode(t *testing.T) {
	for _, s := range stringArgs {
		encoded := append(zlen(s), []byte(s)...)
		r := bytes.NewBuffer(encoded)
		v := stringSchema.Decode(r)
		assert.Equal(t, s, v.(string))
	}
}

func TestArrayEncode(t *testing.T) {
	codec := ArraySchema{ItemSchema: longSchema}
	for _, data := range arrayData {
		var w bytes.Buffer
		codec.Encode(&w, data.a)
		assert.Equal(t, data.b, w.Bytes())
	}
}

func TestArrayDecode(t *testing.T) {
	codec := ArraySchema{ItemSchema: longSchema}
	for _, data := range arrayData {
		r := bytes.NewBuffer(data.b)
		v := codec.Decode(r)
		assert.Equal(t, data.a, v)
	}
}

var intData = []struct {
	v int32
	b []byte
}{
	{0, []byte{0}},
	{1, []byte{2}},
	{2, []byte{4}},
	{-1, []byte{1}},
	{-2, []byte{3}},
	{-64, []byte{0x7F}},
	{62, []byte{124}},
	{64, []byte{0x80, 0x01}},
}

func TestIntEncode(t *testing.T) {
	for _, data := range intData {
		var w bytes.Buffer
		intSchema.Encode(&w, data.v)
		assert.Equal(t, data.b, w.Bytes(), strconv.Itoa(int(data.v)))
	}
}

var boolData = []struct {
	v bool
	b []byte
}{
	{false, []byte{0}},
	{true, []byte{1}},
}

func TestBooleanEncode(t *testing.T) {
	for _, data := range boolData {
		var w bytes.Buffer
		booleanSchema.Encode(&w, data.v)
		assert.Equal(t, data.b, w.Bytes())
	}
}

func TestBooleanDecode(t *testing.T) {
	for _, data := range boolData {
		r := bytes.NewBuffer(data.b)
		v := booleanSchema.Decode(r)
		assert.Equal(t, data.v, v.(bool))
	}
}

var subrecordSchema = RecordSchema{
	Name: "sub",
	Fields: []RecordField{
		{Name: "b", Schema: booleanSchema},
		{Name: "l", Schema: longSchema},
	},
}
var recordData = []struct {
	n string
	c []RecordField
	v []interface{}
	b []byte
}{
	{
		n: "long,long",
		c: []RecordField{RecordField{"a", longSchema}, RecordField{"b", longSchema}},
		v: []interface{}{1, -5},
		b: []byte{2, 9},
	},
	{
		n: "string,long",
		c: []RecordField{RecordField{"a", stringSchema}, RecordField{"b", longSchema}},
		v: []interface{}{"one", 7},
		b: []byte{6, 'o', 'n', 'e', 14},
	},
	// array in record
	{
		n: "long,[]bool",
		c: []RecordField{RecordField{"id", longSchema}, RecordField{"flags", ArraySchema{booleanSchema}}},
		v: []interface{}{3, []interface{}{true, false, true}},
		b: []byte{6, 6, 1, 0, 1, 0},
	},
	//record in record
	{
		n: "name,rec<bool,long>",
		c: []RecordField{
			RecordField{"name", stringSchema},
			RecordField{
				"rec",
				RecordSchema{
					Name: "sub",
					Fields: []RecordField{
						RecordField{"b", booleanSchema},
						RecordField{"l", longSchema},
					},
				},
			},
		},
		v: []interface{}{"two", Record{Schema: subrecordSchema, Values: []interface{}{false, 11}}},
		b: []byte{6, 't', 'w', 'o', 0, 22},
	},
}

func TestRecordEncode(t *testing.T) {
	for _, data := range recordData {
		var w bytes.Buffer
		codec := RecordSchema{Name: "rec", Fields: data.c}
		rec := Record{Schema: codec, Values: data.v}
		codec.Encode(&w, rec)
		assert.Equal(t, data.b, w.Bytes(), data.n)
	}
}

func TestRecordDecode(t *testing.T) {
	for _, data := range recordData {
		r := bytes.NewBuffer(data.b)
		codec := RecordSchema{Name: "rec", Fields: data.c}
		v := codec.Decode(r)
		expected := Record{Schema: codec, Values: data.v}
		assert.Equal(t, expected, v.(Record), data.n)
	}
}

func TestDoubleEncodeDecode(t *testing.T) {
	for _, f := range []float64{0, 1.1, 1.0 / 3.0, 123e4} {
		var w bytes.Buffer
		doubleSchema.Encode(&w, f)
		r := bytes.NewBuffer(w.Bytes())
		v := doubleSchema.Decode(r)
		assert.Equal(t, f, v.(float64))
	}
}
