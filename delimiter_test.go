package pipeline

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"
)

type TestDelimiterEmitter struct {
	string
	int
}

func (t *TestDelimiterEmitter) Emit(w io.Writer) error {
	_, err := w.Write([]byte(fmt.Sprintf("%s\t%v", t.string, t.int)))
	return err
}

func (t *TestDelimiterEmitter) Where() bool {
	return true
}

func TestDelimiterDeserializer(t *testing.T) {

	d := NewDelimiterDeserializer([]byte("\n"), func(b []byte) Emitter {
		ind := bytes.Index(b, []byte(":"))
		if ind == -1 {
			return &TestDelimiterEmitter{}
		}
		key := string(b[:ind])
		value, err := strconv.Atoi(string(b[ind:]))
		if err != nil {
			return &TestDelimiterEmitter{}
		}
		return &TestDelimiterEmitter{key, value}
	})

	testCases := []struct {
		input  []byte
		expect func(Deserializer)
	}{
		{
			input: []byte("test:1"),
			expect: func(d Deserializer) {

			},
		},
	}

	for _, tc := range testCases {
		r := bytes.NewReader(tc.input)
		deserializer := d(r)
		tc.expect(deserializer)
	}

}
