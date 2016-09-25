package avro

import (
	"encoding/json"
)

func baseCodec(name string) Schema {
	switch name {
	case "long":
		return longSchema
	case "bytes":
		return bytesSchema
	case "string":
		return stringSchema
	case "double":
		return doubleSchema
	default:
		panic("unknown codec")
	}
}

func NewCodec(schema string) Schema {
	var parsedSchema interface{}
	check(json.Unmarshal([]byte(schema), &parsedSchema))
	return buildCodec(parsedSchema)
}

func buildField(schema interface{}) RecordField {
	m := schema.(map[string]interface{})
	return RecordField{Name: m["name"].(string), FieldSchema: buildCodec(m["type"])}
}

func buildCodec(schema interface{}) Schema {
	switch v := schema.(type) {
	case string:
		return baseCodec(v)
	case []interface{}:
		var res UnionSchema
		for _, t := range v {
			res.Options = append(res.Options, buildCodec(t))
		}
		return res
	case map[string]interface{}:
		switch v["type"].(string) {
		case "array":
			return ArraySchema{ItemSchema: buildCodec(v["items"])}
		case "record":
			var res RecordSchema
			res.Name = v["name"].(string)
			for _, f := range v["fields"].([]interface{}) {
				res.Fields = append(res.Fields, buildField(f))
			}
			return res
		}
	}
	return nil
}
