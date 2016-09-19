package main

import (
    "fmt"
    "os"
    "io"
    "github.com/elodina/go-avro"
)

func makeRec(i int, schema avro.Schema) *avro.GenericRecord {
    rec := avro.NewGenericRecord(schema)
    rec.Set("id", int32(i))
    rec.Set("name", fmt.Sprintf("name_%d", i))
    return rec
} 

func write(conf Config) {
    var file io.Writer
    if conf.FileName=="" {
        file = os.Stdout
    } else {
        f, err := os.Create(conf.FileName)
        check(err)
        defer f.Close()
        file = f
    }
    datumWriter := avro.NewGenericDatumWriter()
    datumWriter.SetSchema(schema)
    encoder := avro.NewBinaryEncoder(file)
    for i:=0; i<conf.NumRecords; i++ {
        check(datumWriter.Write(makeRec(i, schema), encoder))
    }
}