package pipeline

import (
	"bufio"
	"io"
	"sync"
)

type Merger struct {
	ins []io.Reader
}

func NewMerger() *Merger {
	return &Merger{
		ins: make([]io.Reader, 0),
	}
}

func (m *Merger) In(r io.Reader) *Merger {
	m.ins = append(m.ins, r)
	return m
}

func (m *Merger) Out(w io.Writer) {
	ch := make(chan []byte)
	wg := &sync.WaitGroup{}
	for _, r := range m.ins {
		wg.Add(1)
		go func(r io.Reader) {
			s := bufio.NewScanner(r)
			for s.Scan() {
				ch <- s.Bytes()
			}
			wg.Done()
		}(r)
	}
	go func(w io.Writer) {
		for r := range ch {
			w.Write(r)
			w.Write(NewLine)
		}
	}(w)
	wg.Wait()
	close(ch)
}

func (m *Merger) Then(p Pipeline) Pipeline {
	go func(p Pipeline) {
		m.Out(p)
		p.Close()
	}(p)
	return p
}
