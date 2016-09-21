package main

import (
    "io"
    "fmt"
    "strings"
    "errors"
    "math"
    "encoding/binary"
)

type ValueError struct {
    Value interface{}
    ExpectedType string
}

func (err ValueError) Error() string {
    return fmt.Sprintf("ValueError. Expect %s, found %v of type %T", err.ExpectedType, err.Value, err.Value)
}

type Reader interface {
    io.Reader
    io.ByteReader
}

type Codec interface {
    Encode(w io.Writer, v interface{})
    Decode(r Reader) interface{}
    String() string
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

type LongCodec struct {}

var longCodec LongCodec

func (LongCodec) Encode(w io.Writer, v interface{}) {
    encodeVarInt(w, v.(int))
}

func (LongCodec) Decode(r Reader) interface{} {
    return decodeVarInt(r)
}

func (LongCodec) String() string {
    return "LongCodec"
}

type BytesCodec struct {}

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

var bytesCodec BytesCodec

func (BytesCodec) Encode(w io.Writer, v interface{}) {
    encodeBytes(w, v.([]byte))
}

func (BytesCodec) Decode(r Reader) interface{} {
    return decodeBytes(r)
}

func (BytesCodec) String() string {
    return "BytesCodec"
}

type StringCodec struct{}

var stringCodec StringCodec

func (StringCodec) Encode(w io.Writer, v interface{}) {
    encodeBytes(w, []byte(v.(string)))
}

func (StringCodec) Decode(r Reader) interface{} {
    return string(decodeBytes(r))
}

func (StringCodec) String() string {
    return "StringCodec"
}

type BooleanCodec struct{}

var booleanCodec BooleanCodec

func (BooleanCodec) String() string {
    return "BooleanCodec"
}

func (BooleanCodec) Encode(w io.Writer, v interface{}) {
    b := v.(bool)
    var buf [1]byte
    if b {
        buf[0] = 1
    }
    _, err := w.Write(buf[:])
    check(err)
}

func (BooleanCodec) Decode(r Reader) interface{} {
    var buf [1]byte
    _, err := r.Read(buf[:])
    check(err)
    return buf[0]==1
}

type DoubleCodec struct{}

var doubleCodec DoubleCodec

func (DoubleCodec) String() string {
    return "DoubleCodec"
}

func (DoubleCodec) Encode(w io.Writer, v interface{}) {
    var buf [8]byte
    binary.LittleEndian.PutUint64(buf[:], math.Float64bits(v.(float64)))
    _, err := w.Write(buf[:])
    check(err)
}

func (DoubleCodec) Decode(r Reader) interface{} {
    var buf [8]byte
    _, err := r.Read(buf[:])
    check(err)
    bits := binary.LittleEndian.Uint64(buf[:])
    return math.Float64frombits(bits)
}

type ArrayCodec struct {
    ItemCodec Codec
}

func (codec ArrayCodec) String() string {
    return fmt.Sprintf("ArrayCodec<%s>", codec.ItemCodec)
}

func (codec ArrayCodec) Encode(w io.Writer, v interface{}) {
    arr := v.([]interface{})
    encodeVarInt(w, len(arr))
    for _, item := range(arr) {
        codec.ItemCodec.Encode(w, item)
    }
    _, err := w.Write([]byte{0})
    check(err)
}

func (codec ArrayCodec) Decode(r Reader) interface{} {
    arrLen := decodeVarInt(r)
    buf := make([]interface{}, arrLen)
    for i := range(buf) {
        buf[i] = codec.ItemCodec.Decode(r)
    }
    //TODO: chanked arrays
    b, err := r.ReadByte()
    check(err)
    if b!=byte(0) {
        panic(ValueError{b, "byte(0)"})
    }
    return buf
}

type RecordCodec struct {
    FieldCodecs []Codec
}

func (codec RecordCodec) String() string {
    var codecNames []string
    for _, c := range(codec.FieldCodecs) {
        codecNames = append(codecNames, c.String())
    }
    return fmt.Sprintf("RecordCodec<%s>", strings.Join(codecNames, ","))
}

func (codec RecordCodec) Encode(w io.Writer, v interface{}) {
    items := v.([]interface{})
    if len(items)!=len(codec.FieldCodecs) {
        panic(errors.New(fmt.Sprintf("Record length mismatch. Provided: %d, expected: %d", len(items), len(codec.FieldCodecs))))
    }
    for i, item := range(items) {
        codec.FieldCodecs[i].Encode(w, item)
    }
}

func (codec RecordCodec) Decode(r Reader) interface{} {
    res := make([]interface{}, len(codec.FieldCodecs))
    for i, c := range(codec.FieldCodecs) {
        res[i] = c.Decode(r)
    }
    return res
}