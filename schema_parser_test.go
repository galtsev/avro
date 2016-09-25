package avro

import (
	"bytes"
	"github.com/stretchr/testify/assert"
	"testing"
)

var (
	subRecord = RecordSchema{
		Name: "subrecord",
		Fields: []RecordField{
			{Name: "x", FieldSchema: longSchema},
			{Name: "y", FieldSchema: longSchema},
		},
	}
	parserData = []struct {
		j      string
		schema Schema
	}{
		{`{"type": "array", "items": "string"}`, ArraySchema{ItemSchema: stringSchema}},
		{`{
        "name": "example_3",
        "type": "record",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }`, RecordSchema{
			Name: "example_3",
			Fields: []RecordField{
				{Name: "id", FieldSchema: longSchema},
				{Name: "name", FieldSchema: stringSchema},
			},
		}},
		{`{
        "name": "example_4",
        "type": "record",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "flags", "type": {"type": "array", "items":"string"}},
            {
                "name": "pos",
                "type": {
                    "type": "record",
                    "name": "subrecord",
                    "fields": [
                        {"name": "x", "type": "long"},
                        {"name": "y", "type": "long"}
                    ]
                }
            }
        ]
    }`, RecordSchema{
			Name: "example_4",
			Fields: []RecordField{
				{"id", longSchema},
				{"flags", ArraySchema{ItemSchema: stringSchema}},
				{"pos", subRecord},
			},
		}},
	}
)

func TestNewCodec(t *testing.T) {
	for _, data := range parserData {
		assert.Equal(t, data.schema, NewCodec(data.j))
	}
}

var parserData2 = []struct {
	j     string
	value Record
}{
	{
		j: `{
            "name": "example1",
            "type": "record",
            "fields": [
                {"name": "login", "type": "string"},
                {"name": "age", "type": "int"},
                {"name": "disabled", "type": "boolean"}
            ]

        }`,
		value: Record{Values: []interface{}{"dan", int32(14), false}},
	},
}

func TestParser(t *testing.T) {
	for _, data := range parserData2 {
		schema := NewCodec(data.j)
		var w bytes.Buffer
		schema.Encode(&w, data.value)
		rec := schema.Decode(&w)
		data.value.RecordSchema = schema
		assert.Equal(t, data.value, rec)
	}
}
