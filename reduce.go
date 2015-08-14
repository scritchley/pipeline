package pipeline

import "io"

// Reduce is a reduce process that reads from an io.PipeReader performs a
// reduce operation and writes to an io.PipeWriter
type Reduce struct {
	w                *io.PipeWriter
	r                *io.PipeReader
	DeserializerFunc DeserializerFunc
}

// NewReduce creates and returns a pointer to a new reduce type using the given DeserializerFunc
func NewReduce(d DeserializerFunc) *Reduce {
	pr, pw := io.Pipe()
	return &Reduce{
		w:                pw,
		r:                pr,
		DeserializerFunc: d,
	}
}

// Reduce performs the reduce operation
func (m *Reduce) Reduce(w io.Writer) {
	// Return a channel from the formatter
	ch := deserializeInput(m.DeserializerFunc, m)
	var prev Reducer
	for r := range ch {
		// If nil then continue
		if r == nil {
			continue
		}
		// If Where method returns false then continue
		if !r.Where() {
			continue
		}
		curr, ok := r.(Reducer)
		if !ok {
			continue
		}
		prev = m.reduce(w, prev, curr)
	}
	if prev != nil {
		m.finalize(w, prev)
	}
}

// reduce performs the reduce logic utilised in Reduce. It takes the
// previous and current Emitter, checks whether they contain matching keys
// and if so performs a Sum operation. If the keys do not match it calls
// fn on the Emitter. It returns the current Emitter to be used as the
// previous Emitter on the next iteration of the Reduce func.
func (m *Reduce) reduce(w io.Writer, prev Reducer, curr Reducer) Reducer {
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
func (m *Reduce) finalize(w io.Writer, r Reducer) {
	f, ok := r.(Finalizer)
	if !ok {
		// If keys do not match call fn for previous and set r to previous
		r.(Emitter).Emit(w)
		return
	}
	// If the Emitter implements the Finalizer interface
	// then call the Finalize method passing the underlying writer
	f.Finalize(w)
}

func (m *Reduce) In(r io.Reader) Pipeline {
	go func(w *io.PipeWriter, r io.Reader) {
		io.Copy(m.w, r)
		m.w.Close()
	}(m.w, r)
	return m
}

func (m *Reduce) Then(p Pipeline) Pipeline {
	go func(p Pipeline) {
		m.Reduce(p)
		p.Close()
	}(p)
	return p
}

func (m *Reduce) Out(w io.Writer) {
	m.Reduce(w)
}

func (m *Reduce) Close() error { return m.w.Close() }

func (m *Reduce) Write(p []byte) (int, error) { return m.w.Write(p) }

func (m *Reduce) Read(p []byte) (int, error) { return m.r.Read(p) }
