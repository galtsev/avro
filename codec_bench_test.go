package main

import (
    "testing"
    "math/rand"
    "encoding/json"
    "bytes"
    "fmt"
)

type A struct {
    Name     string
    //BirthDay time.Time
    Phone    string
    Siblings int
    Spouse   bool
    Money    float64
}

func randString(l int) string {
    buf := make([]byte, l)
    for i := 0; i < (l+1)/2; i++ {
        buf[i] = byte(rand.Intn(256))
    }
    return fmt.Sprintf("%x", buf)[:l]
}

func generate() [][]interface{} {
    a := make([][]interface{}, 0, 1000)
    for i := 0; i < 1000; i++ {
        rec := []interface{}{
            randString(16),
            randString(10),
            rand.Intn(5),
            rand.Intn(2)==1,
            rand.Float64(),
        }
        a = append(a, rec)
    }
    return a
}

type Serializer interface {
    Marshal(o interface{}) []byte
    Unmarshal(d []byte, o interface{}) error
    String() string
}

type AvroSerializer struct {
    Codec
}

func (serializer AvroSerializer) Marshal(o interface{}) []byte {
    var a [64]byte
    w := bytes.NewBuffer(a[:0])
    serializer.Encode(w, o)
    return w.Bytes()
}

func (serializer AvroSerializer) Unmarshal(d []byte, o interface{}) error {
    r := bytes.NewBuffer(d)
    serializer.Decode(r)
    return nil
}

func benchMarshal(b *testing.B, s Serializer) {
    b.StopTimer()
    data := generate()
    b.ReportAllocs()
    b.StartTimer()
    for i := 0; i < b.N; i++ {
        s.Marshal(data[rand.Intn(len(data))])
    }
}


func benchUnmarshal(b *testing.B, s Serializer) {
    b.StopTimer()
    data := generate()
    ser := make([][]byte, len(data))
    for i, d := range data {
        o := s.Marshal(d)
        t := make([]byte, len(o))
        copy(t, o)
        ser[i] = t
    }
    b.ReportAllocs()
    b.StartTimer()
    for i := 0; i < b.N; i++ {
        n := rand.Intn(len(ser))
        o := &[]interface{}{}
        err := s.Unmarshal(ser[n], o)
        if err!=nil {
            b.Error(err)
        }
    }
}

var avroSerializer = AvroSerializer{
    RecordCodec{
        Name: "rec",
        Fields: []RecordField{
            RecordField{"name", stringCodec},
            RecordField{"phone", stringCodec},
            RecordField{"siblings", longCodec},
            RecordField{"spouse", booleanCodec},
            RecordField{"money", doubleCodec},
        },
    },
}

func BenchmarkAvroMarshal(b *testing.B) {
    benchMarshal(b, avroSerializer)
}

func BenchmarkAvroUnmarshal(b *testing.B) {
    benchUnmarshal(b, avroSerializer)
}

type JsonSerializer struct{}

func (j JsonSerializer) Marshal(o interface{}) []byte {
    d, _ := json.Marshal(o)
    return d
}

func (j JsonSerializer) Unmarshal(d []byte, o interface{}) error {
    return json.Unmarshal(d, o)
}

func (j JsonSerializer) String() string {
    return "json"
}

func BenchmarkJsonMarshal(b *testing.B) {
    benchMarshal(b, JsonSerializer{})
}

func BenchmarkJsonUnmarshal(b *testing.B) {
    benchUnmarshal(b, JsonSerializer{})
}