package pipeline

import (
	"bufio"
	"bytes"
	"io"
)

// DelimiterDeserializer is a deserializer that splits input rows based on
// the configured delimiter. It is suitable for use with Hadoop Streaming.
type DelimiterDeserializer struct {
	delimiter   string
	constructor func(row []byte) Emitter
	scanner     *bufio.Scanner
}

// HasNext advances the underlying scanner and returns true when data is available. It will return false once no data is available or an error occurs.
func (g *DelimiterDeserializer) HasNext() bool {
	return g.scanner.Scan()
}

// Error returns the last error to occur or nil if there are no errors.
func (g *DelimiterDeserializer) Error() error {
	return g.scanner.Err()
}

// Next retrieves the next available Emitter from the underlying scanner calling the defined constructor method.
func (g *DelimiterDeserializer) Next() Emitter {
	return g.constructor(g.scanner.Bytes())
}

// NewDelimiterDeserializer returns a DeserializerFunc that uses delimiter as the row delimiter and fn as the constructor function.
func NewDelimiterDeserializer(delimiter []byte, fn func([]byte) Emitter) DeserializerFunc {

	// dropCR drops a terminal \r from the data.
	// Taken from bufio/scan.go
	dropCR := func(data []byte) []byte {
		if len(data) > 0 && data[len(data)-1] == '\r' {
			return data[0 : len(data)-1]
		}
		return data
	}

	return func(r io.Reader) Deserializer {

		scanner := bufio.NewScanner(r)
		scanner.Split(func(data []byte, atEOF bool) (int, []byte, error) {
			if atEOF && len(data) == 0 {
				return 0, nil, nil
			}
			if i := bytes.Index(data, delimiter); i >= 0 {
				// We have a full newline-terminated line.
				return i + 1, dropCR(data[0:i]), nil
			}
			// If we're at EOF, we have a final, non-terminated line. Return it.
			if atEOF {
				return len(data), dropCR(data), nil
			}
			// Request more data.
			return 0, nil, nil
		})

		return &DelimiterDeserializer{
			constructor: fn,
			scanner:     scanner,
		}
	}

}
