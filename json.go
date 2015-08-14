package pipeline

import (
	"encoding/gob"
	"encoding/json"
	"io"
)

type JSONDeserializer struct {
	constructor  func() Emitter
	decoder      *json.Decoder
	hasNext      bool
	lastError    error
	ignoreErrors bool
}

func (j *JSONDeserializer) HasNext() bool {
	return j.hasNext
}

func (j *JSONDeserializer) Error() error {
	return j.lastError
}

func (j *JSONDeserializer) Next() Emitter {
	e := j.constructor()
	j.lastError = j.decoder.Decode(e)
	if j.lastError != nil {
		j.hasNext = false
		return nil
	}
	return e
}

func (j *JSONDeserializer) IgnoreMalformedJSON() *JSONDeserializer {
	j.ignoreErrors = true
	return j
}

// NewJSONDeserializer
func NewJSONDeserializer(fn func() Emitter) DeserializerFunc {

	return func(r io.Reader) Deserializer {

		gob.Register(fn())

		// Create a new decoder
		decoder := json.NewDecoder(r)

		return &JSONDeserializer{
			constructor: fn,
			decoder:     decoder,
			hasNext:     true,
			lastError:   nil,
		}

	}

}
