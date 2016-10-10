package binary

import (
	"bytes"
	. "github.com/galtsev/avro"
	"github.com/stretchr/testify/assert"

	"testing"
)

var (
	subRecord = RecordSchema{
		Name: "subrecord",
		Fields: []RecordField{
			{Name: "x", Schema: Long},
			{Name: "y", Schema: Long},
		},
	}
	parserData = []struct {
		j      string
		schema Schema
	}{
		{`{"type": "array", "items": "string"}`, ArraySchema{ItemSchema: String}},
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
				{Name: "id", Schema: Long},
				{Name: "name", Schema: String},
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
				{"id", Long},
				{"flags", ArraySchema{ItemSchema: String}},
				{"pos", subRecord},
			},
		}},
	}
)

func TestNewCodec(t *testing.T) {
	repo := NewRepo()
	for _, data := range parserData {
		schema := repo.Append(data.j)
		assert.Equal(t, data.schema, schema)
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
	repo := NewRepo()
	for _, data := range parserData2 {
		schema := repo.Append(data.j)
		var w bytes.Buffer
		schema.Encode(&w, data.value)
		rec := schema.Decode(&w)
		data.value.Schema = schema
		assert.Equal(t, data.value, rec)
	}
}
