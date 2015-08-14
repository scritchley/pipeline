package pipeline

import (
	"io"
	"sort"
)

type Sorter struct {
	w                *io.PipeWriter
	r                *io.PipeReader
	DeserializerFunc DeserializerFunc
}

// Emitters is a slice of Emitter interfaces that implements the sort.Interface
type Reducers []Reducer

func (e Reducers) Len() int           { return len(e) }
func (e Reducers) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e Reducers) Less(i, j int) bool { return e[i].Key() < e[j].Key() }

func NewSorter(d DeserializerFunc) *Sorter {
	pr, pw := io.Pipe()
	return &Sorter{
		w:                pw,
		r:                pr,
		DeserializerFunc: d,
	}
}

func (m *Sorter) Sort(w io.Writer) {
	// Return a channel from the formatter
	ch := deserializeInput(m.DeserializerFunc, m)
	s := make(Reducers, 0)
	// Iterate over the channel and call fn
	for r := range ch {
		// Continue if Where method returns false
		if !r.Where() {
			continue
		}
		reducer, ok := r.(Reducer)
		if ok {
			s = append(s, reducer)
		}
	}
	sort.Sort(s)
	for _, o := range s {
		o.Emit(w)
	}
}

func (m *Sorter) In(r io.Reader) Pipeline {
	go func(w *io.PipeWriter, r io.Reader) {
		io.Copy(m.w, r)
		m.w.Close()
	}(m.w, r)
	return m
}

func (m *Sorter) Then(p Pipeline) Pipeline {
	go func(p Pipeline) {
		m.Sort(p)
		p.Close()
	}(p)
	return p
}

func (m *Sorter) Out(w io.Writer) {
	m.Sort(w)
}

func (m *Sorter) Close() error { return m.w.Close() }

func (m *Sorter) Write(p []byte) (int, error) { return m.w.Write(p) }

func (m *Sorter) Read(p []byte) (int, error) { return m.r.Read(p) }
