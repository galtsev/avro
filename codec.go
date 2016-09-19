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
    Encode(w io.Writer, v interface{}) error
    Decode(r Reader) (interface{}, error)
    String() string
}

type LongCodec struct {}

var longCodec LongCodec

func (LongCodec) Encode(w io.Writer, v interface{}) error {
    iv, ok := v.(int)
    if !ok {
        return ValueError{v, "int"}
    }
    var buf [10]byte
    l := binary.PutUvarint(buf[:], uint64(zencode(iv)))
    _, err := w.Write(buf[:l])
    return err
}

func (LongCodec) Decode(r Reader) (interface{}, error) {
    v, err := binary.ReadUvarint(r)
    return zdecode(v), err
}

func (LongCodec) String() string {
    return "LongCodec"
}

type BytesCodec struct {}

var bytesCodec BytesCodec

func (BytesCodec) Encode(w io.Writer, v interface{}) error {
    var buf []byte
    var ok bool
    if buf, ok = v.([]byte); !ok {
        return ValueError{v, "[]byte"}
    }
    err := longCodec.Encode(w, len(buf))
    if err!= nil {
        return err
    }
    _, err = w.Write(buf)
    return err
}

func (BytesCodec) Decode(r Reader) (interface{}, error) {
    bufLenV, err := longCodec.Decode(r)
    if err!=nil {
        return nil, err
    }
    bufLen := bufLenV.(int)
    buf := make([]byte, bufLen, bufLen)
    _, err = r.Read(buf)
    if err!=nil {
        return nil, err
    }
    return buf, nil
}

func (BytesCodec) String() string {
    return "BytesCodec"
}

type StringCodec struct{}

var stringCodec StringCodec

func (StringCodec) Encode(w io.Writer, v interface{}) error {
    s, ok := v.(string)
    if !ok {
        return ValueError{v, "string"}
    }
    return bytesCodec.Encode(w, []byte(s))
}

func (StringCodec) Decode(r Reader) (interface{}, error) {
    buf, err := bytesCodec.Decode(r)
    if err!=nil {
        return "", err
    }
    return string(buf.([]byte)), nil
}

func (StringCodec) String() string {
    return "StringCodec"
}

type BooleanCodec struct{}

var booleanCodec BooleanCodec

func (BooleanCodec) String() string {
    return "BooleanCodec"
}

func (BooleanCodec) Encode(w io.Writer, v interface{}) error {
    b, ok := v.(bool)
    if !ok {
        return ValueError{v, "bool"}
    }
    var buf [1]byte
    if b {
        buf[0] = 1
    }
    _, err := w.Write(buf[:])
    return err
}

func (BooleanCodec) Decode(r Reader) (interface{}, error) {
    var buf [1]byte
    _, err := r.Read(buf[:])
    if err!=nil {
        return nil, err
    }
    return buf[0]==1, nil
}

type DoubleCodec struct{}

var doubleCodec DoubleCodec

func (DoubleCodec) String() string {
    return "DoubleCodec"
}

func (DoubleCodec) Encode(w io.Writer, v interface{}) error {
    var buf [8]byte
    f, ok := v.(float64)
    if !ok {
        return ValueError{v, "float64"}
    }
    binary.LittleEndian.PutUint64(buf[:], math.Float64bits(f))
    _, err := w.Write(buf[:])
    return err
}

func (DoubleCodec) Decode(r Reader) (interface{}, error) {
    var buf [8]byte
    _, err := r.Read(buf[:])
    if err!=nil {
        return nil, err
    }
    bits := binary.LittleEndian.Uint64(buf[:])
    f := math.Float64frombits(bits)
    return f, nil
}

type ArrayCodec struct {
    ItemCodec Codec
}

func (codec ArrayCodec) String() string {
    return fmt.Sprintf("ArrayCodec<%s>", codec.ItemCodec)
}

func (codec ArrayCodec) Encode(w io.Writer, v interface{}) error {
    arr, ok := v.([]interface{})
    if !ok {
        return ValueError{v, "[]interface{}"}
    }
    err := longCodec.Encode(w, len(arr))
    if err!=nil {
        return err
    }
    for _, item := range(arr) {
        err:=codec.ItemCodec.Encode(w, item)
        if err!=nil {
            return err
        }
    }
    err = longCodec.Encode(w, 0)
    if err!=nil {
        return err
    }
    return nil
}

func (codec ArrayCodec) Decode(r Reader) (interface{}, error) {
    arrLenI, err := longCodec.Decode(r)
    if err!=nil {
        return nil, err
    }
    arrLen, ok := arrLenI.(int)
    if !ok {
        return nil, ValueError{arrLenI, "int"}
    }
    buf := make([]interface{}, arrLen)
    for i := range(buf) {
        v, err:= codec.ItemCodec.Decode(r)
        if err!=nil {
            return nil, err
        }
        buf[i] = v
    }
    //TODO: chanked arrays
    zero, err := longCodec.Decode(r)
    if err!=nil {
        return nil, err
    }
    if zero.(int)!=0 {
        return nil, ValueError{zero, "int(0)"}
    }
    return buf, nil
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

func (codec RecordCodec) Encode(w io.Writer, v interface{}) error {
    items, ok := v.([]interface{})
    if !ok {
        return ValueError{v, "[]interface{}"}
    }
    if len(items)!=len(codec.FieldCodecs) {
        return errors.New(fmt.Sprintf("Record length mismatch. Provided: %d, expected: %d", len(items), len(codec.FieldCodecs)))
    }
    for i, item := range(items) {
        err := codec.FieldCodecs[i].Encode(w, item)
        if err!=nil {
            return err
        }
    }
    return nil
}

func (codec RecordCodec) Decode(r Reader) (interface{}, error) {
    res := make([]interface{}, len(codec.FieldCodecs))
    for i, c := range(codec.FieldCodecs) {
        v, err := c.Decode(r)
        if err!=nil {
            return nil, err
        }
        res[i] = v
    }
    return res, nil
}