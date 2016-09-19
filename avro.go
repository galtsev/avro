package main

import (
    "flag"
    "fmt"
)
func main() {
    var conf Config
    flag.StringVar(&conf.FileName, "f", "", "data file")
    flag.IntVar(&conf.NumRecords, "n", 100, "number of records in data file")
    flag.Parse()
    cmd := flag.Arg(0)
    switch cmd {
    case "write":
        write(conf)
    case "dump":
        readAndDump(conf)
    default:
        fmt.Println("Known commands: write, dump")
    }
    fmt.Println("ok")

}