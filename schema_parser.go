package main

import (
    "encoding/json"
)

func baseCodec(name string) Codec {
    switch name {
    case "long":
        return longCodec
    case "bytes":
        return bytesCodec
    case "string":
        return stringCodec
    case "double":
        return doubleCodec
    default:
        panic("unknown codec")
    }
}

func NewCodec(schema string) Codec {
    var parsedSchema interface{}
    check(json.Unmarshal([]byte(schema), &parsedSchema))
    return buildCodec(parsedSchema)
}

func buildField(schema interface{}) RecordField {
    m := schema.(map[string]interface{})
    return RecordField{Name: m["name"].(string), FieldCodec: buildCodec(m["type"])}
}

func buildCodec(schema interface{}) Codec {
    switch v := schema.(type) {
    case string:
        return baseCodec(v)
    case []interface{}:
        var res UnionCodec
        for _, t := range(v) {
            res.Options = append(res.Options, buildCodec(t))
        }
        return res
    case map[string]interface{}:
        switch v["type"].(string) {
        case "array":
            return ArrayCodec{ItemCodec: buildCodec(v["items"])}
        case "record":
            var res RecordCodec
            res.Name = v["name"].(string)
            for _, f := range(v["fields"].([]interface{})) {
                res.Fields = append(res.Fields, buildField(f))
            }
            return res
        }
    }
    return nil
}

