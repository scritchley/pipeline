package pipeline

import (
	"io"
)

// Map implements the Pipeline interface. It deserializes an input
// using a Deserializer and emits a key value pair of resulting Emitter
// interface.
type Map struct {
	w                *io.PipeWriter
	r                *io.PipeReader
	DeserializerFunc DeserializerFunc
}

// NewMap creates a new Map with the given Deserializer d
func NewMap(d DeserializerFunc) *Map {
	pr, pw := io.Pipe()
	return &Map{
		w:                pw,
		r:                pr,
		DeserializerFunc: d,
	}
}

// Out writes the Map output to w. It returns a channel of Emitters
// from the deserializer and then emits the Emitters Key and Value to the
// output.
func (m *Map) Out(w io.Writer) {
	// Return a channel from the formatter
	ch := deserializeInput(m.DeserializerFunc, m)
	// Iterate over the channel and call fn
	for r := range ch {
		if r == nil {
			continue
		}
		// Continue if Where method returns false
		if !r.Where() {
			continue
		}
		// Call emit passing in the output writer
		r.Emit(w)
	}
}

// In reads from input r into the Map and returns
// the Map.
func (m *Map) In(r io.Reader) Pipeline {
	go func(w *io.PipeWriter, r io.Reader) {
		io.Copy(m.w, r)
		m.w.Close()
	}(m.w, r)
	return m
}

// Then starts a goroutine to write the output of the Map to the
// given Pipeline and returns it.
func (m *Map) Then(p Pipeline) Pipeline {
	go func(p Pipeline) {
		m.Out(p)
		p.Close()
	}(p)
	return p
}

func (m *Map) Close() error { return m.w.Close() }

func (m *Map) Write(p []byte) (int, error) { return m.w.Write(p) }

func (m *Map) Read(p []byte) (int, error) { return m.r.Read(p) }
