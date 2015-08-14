package pipeline

import (
	"encoding/gob"
	"io"
)

type GOBDeserializer struct {
	constructor func() Emitter
	decoder     *gob.Decoder
	hasNext     bool
	lastError   error
}

func (g *GOBDeserializer) HasNext() bool {
	return g.hasNext
}

func (g *GOBDeserializer) Error() error {
	return g.lastError
}

func (g *GOBDeserializer) Next() Emitter {
	e := g.constructor()
	g.lastError = g.decoder.Decode(e)
	if g.lastError != nil {
		g.hasNext = false
		return nil
	}
	return e
}

// NewGOBDeserializer
func NewGOBDeserializer(fn func() Emitter) DeserializerFunc {

	return func(r io.Reader) Deserializer {

		// Create a new decoder
		decoder := gob.NewDecoder(r)

		return &GOBDeserializer{
			constructor: fn,
			decoder:     decoder,
			hasNext:     true,
			lastError:   nil,
		}

	}

}
