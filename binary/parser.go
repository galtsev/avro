package binary

import (
	"encoding/json"
	. "github.com/galtsev/avro"
)

type BinarySchemaRepo struct {
	schemas map[string]Schema
}

func NewRepo() SchemaRepo {
	repo := BinarySchemaRepo{schemas: make(map[string]Schema)}
	repo.AppendSchema("boolean", booleanSchema)
	repo.AppendSchema("int", intSchema)
	repo.AppendSchema("long", longSchema)
	repo.AppendSchema("bytes", bytesSchema)
	repo.AppendSchema("string", stringSchema)
	repo.AppendSchema("double", doubleSchema)
	return &repo
}

func (r *BinarySchemaRepo) buildField(schema interface{}) RecordField {
	m := schema.(map[string]interface{})
	return RecordField{Name: m["name"].(string), Schema: r.buildCodec(m["type"])}
}

func (r *BinarySchemaRepo) buildCodec(schema interface{}) Schema {
	switch v := schema.(type) {
	case string:
		return r.Get(v)
	case []interface{}:
		var res UnionSchema
		for _, t := range v {
			res.Options = append(res.Options, r.buildCodec(t))
		}
		return res
	case map[string]interface{}:
		switch v["type"].(string) {
		case "array":
			return ArraySchema{ItemSchema: r.buildCodec(v["items"])}
		case "record":
			var res RecordSchema
			res.Name = v["name"].(string)
			for _, f := range v["fields"].([]interface{}) {
				res.Fields = append(res.Fields, r.buildField(f))
			}
			return res
		}
	}
	return nil
}

func (r *BinarySchemaRepo) AppendSchema(name string, schema Schema) {
	r.schemas[name] = schema
}

func (r *BinarySchemaRepo) Append(j string) Schema {
	var parsedSchema interface{}
	check(json.Unmarshal([]byte(j), &parsedSchema))
	schema := r.buildCodec(parsedSchema)
	name := schema.SchemaName()
	r.AppendSchema(name, schema)
	return schema
}

func (r *BinarySchemaRepo) Get(name string) Schema {
	return r.schemas[name]
}

var _ SchemaRepo = &BinarySchemaRepo{}
