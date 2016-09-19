package main

import (
    "fmt"
    "io/ioutil"
    "os"
    "io"
    "github.com/elodina/go-avro"
)

func readAndDump(conf Config) {
    var file io.Reader
    if conf.FileName=="" {
        file = os.Stdin
    } else {
        f, err := os.Open(conf.FileName)
        check(err)
        defer f.Close()
        file = f
    }
    data, err := ioutil.ReadAll(file)
    check(err)
    datumReader := avro.NewGenericDatumReader()
    datumReader.SetSchema(schema)
    decoder := avro.NewBinaryDecoder(data)
    for i:=0; i<conf.NumRecords; i++ {
        rec := avro.NewGenericRecord(schema)
        check(datumReader.Read(rec, decoder))
        fmt.Println(rec)
    }

}

