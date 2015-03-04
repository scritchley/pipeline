package pipeline

import (
	"bytes"
	"strconv"
	"testing"
)

type testData struct {
	K string `json:"key"`
	V int    `json:"value"`
}

func (t *testData) Filter() bool {
	return true
}

func (t *testData) Key() string {
	return t.K
}

func (t *testData) Value() string {
	return strconv.Itoa(t.V)
}

func (t *testData) Sum(s Emitter) {
	d := s.(*testData)
	t.V += d.V
}

func TestMapper(t *testing.T) {

	input := "{\"key\":\"test\",\"value\":1}\n"
	expectedOutput := "test\t1\n"

	r := bytes.NewBufferString(input)
	w := &bytes.Buffer{}
	mapper := NewMapper(JSONDeserializer(func() Emitter {
		return &testData{}
	}))
	mapper.In(r).Out(w)
	if w.String() != expectedOutput {
		println(w.String(), expectedOutput)
		t.Error("Test failed, output did not match expected")
	}

}

func TestReducer(t *testing.T) {

	input := "{\"key\":\"test\",\"value\":1}\n{\"key\":\"test\",\"value\":1}"
	expectedOutput := "test\t2\n"

	r := bytes.NewBufferString(input)
	w := &bytes.Buffer{}
	reducer := NewReducer(JSONDeserializer(func() Emitter {
		return &testData{}
	}))
	reducer.In(r).Out(w)
	if w.String() != expectedOutput {
		println(w.String(), expectedOutput)
		t.Error("Test failed, output did not match expected")
	}

}

func TestSorter(t *testing.T) {

	input := "{\"key\":\"A\",\"value\":1}\n{\"key\":\"B\",\"value\":1}"
	expectedOutput := "A\t1\nB\t1\n"

	r := bytes.NewBufferString(input)
	w := &bytes.Buffer{}
	sorter := NewSorter(JSONDeserializer(func() Emitter {
		return &testData{}
	}))
	sorter.In(r).Out(w)
	if w.String() != expectedOutput {
		println(w.String(), expectedOutput)
		t.Error("Test failed, output did not match expected")
	}

}
