package pipeline

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
)

// Deserializer should take an empty interface and return an Emitter.
// It is used within the Map and Reduce functions to deserialize incoming
// data into an type that implements the Emitter interface.
type Deserializer func([]byte) Emitter

func deserializeInput(d Deserializer) func(io.Reader) chan Emitter {
	return func(r io.Reader) chan Emitter {
		// Create a channel of Emitter to send incoming deserialized []bytes
		ch := make(chan Emitter)
		go func(d Deserializer, s *bufio.Scanner) {
			for s.Scan() {
				ch <- d(s.Bytes())
			}
			if err := s.Err(); err != nil {
				fmt.Fprintln(os.Stderr, "reading input:", err)
			}
			close(ch)
		}(d, bufio.NewScanner(r))
		return ch
	}
}

// JSONDeserializer returns a Deserializer that parses a byte slice and
// unmarshals it to the Emitter type provided by calling fn.
func JSONDeserializer(fn func() Emitter) Deserializer {
	return func(b []byte) Emitter {
		// Find a tab delimiter (the hadoop default key field delimiter)
		delim := bytes.Index(b, DefaultKeyFieldDelimiter)
		// Create a new row, could look at implementing sync.Pool for this
		s := fn()
		// If the delimiter is present separate into key and value portions
		if delim != -1 {
			b = b[delim+1:]
		}
		json.Unmarshal(b, &s)
		return s
	}
}
