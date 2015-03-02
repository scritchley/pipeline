package pipeline

import (
	"bytes"
	"os"
	"strconv"
	"testing"
)

func testEmitterDeserializer(b []byte) Emitter {
	delim := bytes.IndexRune(b, '\t')
	val, _ := strconv.Atoi(string(b[delim+1:]))
	return &testEmitter{
		K: string(b[:delim]),
		V: val,
	}
}

type testEmitter struct {
	K string `json:"key"`
	V int    `json:"value"`
}

func (t *testEmitter) Key() string {
	return t.K
}

func (t *testEmitter) Value() string {
	return strconv.Itoa(t.V)
}

func (t *testEmitter) Filter() bool {
	return t.V == 1
}

func (t *testEmitter) Sum(s Emitter) {
	d := s.(*testEmitter)
	t.V += d.V
}

func (t *testEmitter) Finalize() string {
	return strconv.Itoa(t.V * 2)
}

func TestMap(t *testing.T) {
	input := "foo\t1"
	expectedOutput := "foo\t1\n"

	r := bytes.NewBufferString(input)
	w := &bytes.Buffer{}
	mr := New(r, w)
	mr.Deserializer(testEmitterDeserializer)
	mr.Map()
	if w.String() != expectedOutput {
		t.Error("Test failed, output did not match expected")
	}
}

func TestReduce(t *testing.T) {
	input := "foo\t1\nfoo\t1"
	expectedOutput := "foo\t4\n" // Should return 4 as the finalizer multiplies the summed value by 2

	r := bytes.NewBufferString(input)
	w := &bytes.Buffer{}
	mr := New(r, w)
	mr.Deserializer(testEmitterDeserializer)
	mr.Reduce()
	if w.String() != expectedOutput {
		t.Error("Test failed, output did not match expected")
	}
}

func TestFilter(t *testing.T) {
	input := "foo\t1\nfoo\t2"
	expectedOutput := "foo\t2\n" // Should return 2 as it filters out values other than 1 and the finalizer multiplies the summed value by 2

	r := bytes.NewBufferString(input)
	w := &bytes.Buffer{}
	mr := New(r, w)
	mr.Deserializer(testEmitterDeserializer)
	mr.Reduce()
	if w.String() != expectedOutput {
		t.Error("Test failed, output did not match expected")
	}
}

func BenchmarkReduce(b *testing.B) {
	prev := &testEmitter{
		K: "test",
		V: 1,
	}
	curr := &testEmitter{
		K: "test",
		V: 1,
	}
	mr := New(os.Stdin, os.Stderr)
	mr.Deserializer(testEmitterDeserializer)
	for i := 0; i < b.N; i++ {
		mr.reduce(prev, curr)
	}
}
