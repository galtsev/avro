package ocf

import (
	"bytes"
	"github.com/galtsev/avro"
	"github.com/galtsev/avro/binary"
	"io"
	"math/rand"
)

func check(err error) {
	if err != nil {
		panic(err)
	}
}

type Writer struct {
	writer       io.Writer
	buf          bytes.Buffer
	recsInBuffer int
	schema       avro.Schema
	jschema      string
	BatchSize    int
	syncString   [16]byte
}

func (fw *Writer) WriteHeader() {
	fw.writer.Write([]byte("Obj\x01"))
	header := make(map[string]interface{})
	header["avro.schema"] = []byte(fw.jschema)
	header["avro.codec"] = []byte("null")
	headerSchema := binary.MapSchema{ValueSchema: binary.Bytes}
	headerSchema.Encode(fw.writer, header)
	_, err := fw.writer.Write(fw.syncString[:])
	check(err)
}

func (fw *Writer) Write(v interface{}) {
	fw.schema.Encode(&fw.buf, v)
	fw.recsInBuffer += 1
	if fw.recsInBuffer >= fw.BatchSize {
		fw.Flush()
	}
}

func (fw *Writer) Flush() {
	binary.EncodeVarInt(fw.writer, fw.recsInBuffer)
	binary.EncodeVarInt(fw.writer, len(fw.buf.Bytes()))
	io.Copy(fw.writer, &fw.buf)
	_, err := fw.writer.Write(fw.syncString[:])
	check(err)
	fw.recsInBuffer = 0
	fw.buf.Reset()
}

func NewWriter(w io.Writer, schema string) *Writer {
	repo := binary.NewRepo()
	res := Writer{
		writer:    w,
		jschema:   schema,
		schema:    repo.Append(schema),
		BatchSize: 1000,
	}
	rand.Read(res.syncString[:])
	return &res
}
