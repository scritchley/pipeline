package pipeline

import (
	"io"
)

// Mapper implements the Pipeline interface. It deserializes an input
// using a Deserializer and emits a key value pair of resulting Emitter
// interface.
type Mapper struct {
	w            *io.PipeWriter
	r            *io.PipeReader
	Deserializer Deserializer
}

// NewMapper creates a new Mapper with the given Deserializer d
func NewMapper(d Deserializer) *Mapper {
	pr, pw := io.Pipe()
	return &Mapper{
		w:            pw,
		r:            pr,
		Deserializer: d,
	}
}

// Out writes the Mapper output to w. It returns a channel of Emitters
// from the deserializer and then emits the Emitters Key and Value to the
// output.
func (m *Mapper) Out(w io.Writer) {
	// Return a channel from the formatter
	ch := deserializeInput(m.Deserializer)(m)
	// Iterate over the channel and call fn
	for r := range ch {
		// Continue if Filter method returns false
		if !r.Filter() {
			continue
		}
		emit(w, r.Key(), r.Value())
	}
}

// In reads from input r into the Mapper and returns
// the Mapper.
func (m *Mapper) In(r io.Reader) Pipeline {
	go func(w *io.PipeWriter, r io.Reader) {
		io.Copy(m.w, r)
		m.w.Close()
	}(m.w, r)
	return m
}

// Then starts a goroutine to write the output of the Mapper to the
// given Pipeline and returns it.
func (m *Mapper) Then(p Pipeline) Pipeline {
	go func(p Pipeline) {
		m.Out(p)
		p.Close()
	}(p)
	return p
}

func (m *Mapper) Close() error { return m.w.Close() }

func (m *Mapper) Write(p []byte) (int, error) { return m.w.Write(p) }

func (m *Mapper) Read(p []byte) (int, error) { return m.r.Read(p) }
