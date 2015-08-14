package pipeline

import (
	"io"

	"gopkg.in/vmihailenco/msgpack.v2"
)

type MsgPackDeserializer struct {
	constructor func() Emitter
	decoder     *msgpack.Decoder
	hasNext     bool
	lastError   error
}

func (m *MsgPackDeserializer) HasNext() bool {
	return m.hasNext
}

func (m *MsgPackDeserializer) Error() error {
	return m.lastError
}

func (m *MsgPackDeserializer) Next() Emitter {
	e := m.constructor()
	m.lastError = m.decoder.Decode(e)
	if m.lastError != nil {
		m.hasNext = false
	}
	return e
}

func NewMsgPackDeserializer(fn func() Emitter) DeserializerFunc {

	return func(r io.Reader) Deserializer {

		// Create a new decoder
		decoder := msgpack.NewDecoder(r)

		return &MsgPackDeserializer{
			constructor: fn,
			decoder:     decoder,
			hasNext:     true,
			lastError:   nil,
		}

	}

}
