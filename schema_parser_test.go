package avro

import (
	_ "github.com/stretchr/testify/assert"
	"testing"
)

var (
	//example_1 = "long"
	example_2 = `{"type": "array", "items": "string"}`
	example_3 = `{
        "name": "example_3",
        "type": "record",
        "fields": [
            {"name": "id", "type": "long"},
            {"name": "name", "type": "string"}
        ]
    }`
	example_4 = `{
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
    }`
)

func TestNewCodec(t *testing.T) {
	for _, example := range []string{example_2, example_3, example_4} {
		_ = NewCodec(example)
	}
}
