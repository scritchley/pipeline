package pipeline

import (
	"io"
	"sync"
)

type Workflow struct {
	inputs []io.Reader
	output io.Writer
	wg     *sync.WaitGroup
	stages []MapReducer
}

func NewWorkflow() *Workflow {
	return &Workflow{
		wg: &sync.WaitGroup{},
	}
}

func (w *Workflow) AddInput(r io.Reader) {
	w.inputs = append(w.inputs, r)
}

func (w *Workflow) AddMapStage(d Deserializer) {
	w.stages = append(w.stages, MapReducer{})
}
