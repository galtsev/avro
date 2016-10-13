package ocf

import (
	"bytes"
	"github.com/galtsev/avro"
	"github.com/galtsev/avro/binary"
	"io"
)

type Reader struct {
	reader       avro.Reader
	buf          bytes.Buffer
	schema       avro.Schema
	recsInBuffer int
	Value        interface{}
}

func NewReader(r avro.Reader) *Reader {
	res := Reader{reader: r}
	var magic [4]byte
	_, err := r.Read(magic[:])
	check(err)
	headerSchema := binary.MapSchema{ValueSchema: binary.Bytes}
	header := headerSchema.Decode(r).(map[string]interface{})
	jschema := string(header["avro.schema"].([]byte))
	repo := binary.NewRepo()
	res.schema = repo.Append(jschema)
	var sync [16]byte
	_, err = r.Read(sync[:])
	check(err)
	return &res
}

func (r *Reader) NextBatch() (ok bool) {
	defer func() {
		err := recover()
		if err == nil {
			ok = true
			return
		}
		if err == io.EOF {
			ok = false
			return
		}
		panic(err)
	}()
	r.recsInBuffer = binary.DecodeVarInt(r.reader)
	blockLen := binary.DecodeVarInt(r.reader)
	buf := make([]byte, blockLen)
	_, err := io.ReadFull(r.reader, buf)
	check(err)
	var sync [16]byte
	_, err = io.ReadFull(r.reader, sync[:])
	check(err)
	r.buf = *bytes.NewBuffer(buf)
	return true
}

func (r *Reader) Next() bool {
	if r.recsInBuffer == 0 {
		return false
	}
	r.Value = r.schema.Decode(&r.buf)
	r.recsInBuffer -= 1
	return true
}
