package pipeline

import (
	"bytes"
	"fmt"
	"io"
	"strconv"
	"testing"
)

// testData is a test type that implements the required interfaces for the below tests.
type testData struct {
	K string `json:"key"`
	V int    `json:"value"`
}

func (t *testData) Where() bool {
	return true
}

func (t *testData) Emit(w io.Writer) error {
	_, err := w.Write([]byte(fmt.Sprintf("%s\t%v\n", t.K, t.V)))
	return err
}

func (t *testData) Key() string {
	return t.K
}

func (t *testData) Value() string {
	return strconv.Itoa(t.V)
}

func (t *testData) Sum(s ...Emitter) {
	for _, e := range s {
		d := e.(*testData)
		t.V += d.V
	}
}

func TestMapper(t *testing.T) {

	input := "{\"key\":\"test\",\"value\":1}\n"
	expectedOutput := "test\t1\n"

	r := bytes.NewBufferString(input)
	w := &bytes.Buffer{}
	mapper := NewMap(NewJSONDeserializer(func() Emitter {
		return &testData{}
	}))
	mapper.In(r).Out(w)
	if w.String() != expectedOutput {
		println(w.String(), expectedOutput)
		t.Error("Test failed, output did not match expected")
	}

}

func TestReducer(t *testing.T) {

	input := "{\"key\":\"test\",\"value\":1}\n{\"key\":\"test\",\"value\":1}\n"
	expectedOutput := "test\t2\n"

	r := bytes.NewBufferString(input)
	w := &bytes.Buffer{}
	reducer := NewReduce(NewJSONDeserializer(func() Emitter {
		return &testData{}
	}))
	reducer.In(r).Out(w)
	if w.String() != expectedOutput {
		println(w.String(), expectedOutput)
		t.Error("Test failed, output did not match expected")
	}

}
