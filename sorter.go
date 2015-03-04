package pipeline

import (
	"io"
	"sort"
)

type Sorter struct {
	w            *io.PipeWriter
	r            *io.PipeReader
	Deserializer Deserializer
}

// Emitters is a slice of Emitter interfaces that implements the sort.Interface
type Emitters []Emitter

func (e Emitters) Len() int           { return len(e) }
func (e Emitters) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e Emitters) Less(i, j int) bool { return e[i].Key() < e[j].Key() }

func NewSorter(d Deserializer) *Sorter {
	pr, pw := io.Pipe()
	return &Sorter{
		w:            pw,
		r:            pr,
		Deserializer: d,
	}
}

func (m *Sorter) Sort(w io.Writer) {
	// Return a channel from the formatter
	ch := deserializeInput(m.Deserializer)(m)
	s := make(Emitters, 0)
	// Iterate over the channel and call fn
	for r := range ch {
		// Continue if Filter method returns false
		if !r.Filter() {
			continue
		}
		s = append(s, r)
	}
	sort.Sort(s)
	for _, o := range s {
		emit(w, o.(Emitter).Key(), o.Value())
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
