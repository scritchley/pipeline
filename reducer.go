package pipeline

import (
	"io"
)

type Reducer struct {
	w            *io.PipeWriter
	r            *io.PipeReader
	Deserializer Deserializer
}

func NewReducer(d Deserializer) *Reducer {
	pr, pw := io.Pipe()
	return &Reducer{
		w:            pw,
		r:            pr,
		Deserializer: d,
	}
}

func (m *Reducer) Reduce(w io.Writer) {
	// Return a channel from the formatter
	ch := deserializeInput(m.Deserializer)(m)
	var prev Summer
	for r := range ch {
		// Continue if Filter method returns false
		if !r.Filter() {
			continue
		}
		curr, ok := r.(Summer)
		if !ok {
			continue
		}
		prev = m.reduce(w, prev, curr)
	}
	if prev != nil {
		m.finalize(w, prev)
	}
}

// reduce performs the reducer logic utilised in Reduce. It takes the
// previous and current Emitter, checks whether they contain matching keys
// and if so performs a Sum operation. If the keys do not match it calls
// fn on the Emitter. It returns the current Emitter to be used as the
// previous Emitter on the next iteration of the Reduce func.
func (m *Reducer) reduce(w io.Writer, prev Summer, curr Summer) Summer {
	// If nil, then first run, set r to previous and return
	if prev == nil {
		return curr
	}
	// If keys match then perform sum operation and return
	if prev.Key() == curr.Key() {
		prev.Sum(curr)
		return prev
	}
	m.finalize(w, prev)
	return curr
}

// finalize checks whether the given Summer implements a
// Finalizer interface and if so calls the Finalize method.
func (m *Reducer) finalize(w io.Writer, r Summer) {
	f, ok := r.(Finalizer)
	if !ok {
		// If keys do not match call fn for previous and set r to previous
		emit(w, r.(Emitter).Key(), r.(Emitter).Value())
		return
	}
	// If the Emitter implements the Finalizer interface
	// then call the Finalize method
	emit(w, r.(Emitter).Key(), f.Finalize())
}

func (m *Reducer) In(r io.Reader) Pipeline {
	go func(w *io.PipeWriter, r io.Reader) {
		io.Copy(m.w, r)
		m.w.Close()
	}(m.w, r)
	return m
}

func (m *Reducer) Then(p Pipeline) Pipeline {
	go func(p Pipeline) {
		m.Reduce(p)
		p.Close()
	}(p)
	return p
}

func (m *Reducer) Out(w io.Writer) {
	m.Reduce(w)
}

func (m *Reducer) Close() error { return m.w.Close() }

func (m *Reducer) Write(p []byte) (int, error) { return m.w.Write(p) }

func (m *Reducer) Read(p []byte) (int, error) { return m.r.Read(p) }
