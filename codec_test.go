package main

import (
    "bytes"
    "testing"
    "github.com/stretchr/testify/assert"
)


var (
    longData = []struct{
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
    arrayData = []struct{
        a []interface{}
        b []byte
    } {
        {[]interface{}{}, []byte{0, 0}},
        {[]interface{}{0}, []byte{2, 0, 0}},
        {[]interface{}{1,-2}, []byte{4, 2, 3, 0}},
    }
)

func TestLongCodecEncode(t *testing.T) {
    for _, data := range(longData) {
        var w bytes.Buffer
        longCodec.Encode(&w, data.i)
        assert.Equal(t, data.b, w.Bytes())
    }
}


func TestLongCodecDecode(t *testing.T) {
    for _, data := range(longData) {
        buf := bytes.NewBuffer(data.b)
        v := longCodec.Decode(buf)
        assert.Equal(t, data.i, v.(int))
    }
}

func zlen(s string) []byte {
    return []byte{byte(zencode(len(s)))}
}

func TestStringCodecEncode(t *testing.T) {
    for _, s := range(stringArgs) {
        var w bytes.Buffer
        stringCodec.Encode(&w, s)
        expected := append(zlen(s), []byte(s)...)
        assert.Equal(t, expected, w.Bytes())
    }
}

func TestStringCodecDecode(t *testing.T) {
    for _, s := range(stringArgs) {
        encoded := append(zlen(s), []byte(s)...)
        r := bytes.NewBuffer(encoded)
        v := stringCodec.Decode(r)
        assert.Equal(t, s, v.(string))
    }
}

func TestArrayEncode(t *testing.T) {
    codec := ArrayCodec{ItemCodec: longCodec}
    for _, data := range(arrayData) {
        var w bytes.Buffer
        codec.Encode(&w, data.a)
        assert.Equal(t, data.b, w.Bytes())
    }
}

func TestArrayDecode(t *testing.T) {
    codec := ArrayCodec{ItemCodec: longCodec}
    for _, data := range(arrayData) {
        r := bytes.NewBuffer(data.b)
        v := codec.Decode(r)
        assert.Equal(t, data.a, v)
    }
}

var boolData = []struct{
    v bool
    b []byte
} {
    {false, []byte{0}},
    {true, []byte{1}},
    
}

func TestBooleanEncode(t *testing.T) {
    for _, data := range(boolData) {
        var w bytes.Buffer
        booleanCodec.Encode(&w, data.v)
        assert.Equal(t, data.b, w.Bytes())
    }
}

func TestBooleanDecode(t *testing.T) {
    for _, data := range(boolData) {
        r := bytes.NewBuffer(data.b)
        v := booleanCodec.Decode(r)
        assert.Equal(t, data.v, v.(bool))
    }
}

var recordData = []struct{
    c []Codec
    v []interface{}
    b []byte
} {
    {
        c: []Codec{longCodec, longCodec},
        v: []interface{}{1, -5},
        b: []byte{2, 9},
    },
    {
        c: []Codec{stringCodec, longCodec},
        v: []interface{}{"one", 7},
        b: []byte{6,'o','n','e',14},
    },
    // array in record
    {
        c: []Codec{longCodec, ArrayCodec{booleanCodec}},
        v: []interface{}{3, []interface{}{true,false,true}},
        b: []byte{6,6,1,0,1,0},
    },
    //record in record
    {
        c: []Codec{stringCodec, RecordCodec{FieldCodecs: []Codec{booleanCodec,longCodec}} },
        v: []interface{}{"two", []interface{}{false, 11}},
        b: []byte{6,'t','w','o',0,22},
    },
}

func TestRecordEncode(t *testing.T) {
    for _, data := range(recordData) {
        var w bytes.Buffer
        codec := RecordCodec{FieldCodecs: data.c}
        codec.Encode(&w, data.v)
        assert.Equal(t, data.b, w.Bytes())
    }
}

func TestRecordDecode(t *testing.T) {
    for _, data := range(recordData) {
        r := bytes.NewBuffer(data.b)
        codec := RecordCodec{FieldCodecs: data.c}
        v := codec.Decode(r)
        assert.Equal(t, data.v, v.([]interface{}))
    }
}

func TestDoubleEncodeDecode(t *testing.T) {
    for _, f := range([]float64{0, 1.1, 1.0/3.0, 123e4}) {
        var w bytes.Buffer
        doubleCodec.Encode(&w, f)
        r := bytes.NewBuffer(w.Bytes())
        v := doubleCodec.Decode(r)
        assert.Equal(t, f, v.(float64))
    }
}