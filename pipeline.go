package pipeline

import (
	"fmt"
	"hash/fnv"
	"io"
)

// Pipeline implements the required methods for a pipeline stage.
type Pipeline interface {
	io.ReadWriteCloser
	// In takes an io.Reader and returns a instance of the Pipeline
	// interface. It is used for providing an input, such as a file,
	// to a Pipeline
	In(io.Reader) Pipeline
	// Then writes the output of the Pipeline to another Pipeline and returns
	// that Pipeline. It is used for chaining together stages of the pipeline.
	Then(Pipeline) Pipeline
	// Out writes the output of this Pipeline to the given io.Writer
	Out(io.Writer)
}

var DefaultKeyFieldDelimiter = []byte("\t")
var NewLine = []byte("\n")

// Hash returns a hashed string of the given keys. Used
// for creating compound keys when grouping by multiple
// fields.
func Hash(keys ...string) string {
	h := fnv.New64()
	for _, k := range keys {
		h.Write([]byte(k))
	}
	return fmt.Sprint(h.Sum64())
}

// Emitter implements methods required within the Map stage.
type Emitter interface {
	// Emit is called passing in the underlying writer. It's upto the caller
	// to write data in the required format.
	Emit(w io.Writer) error
	// Where should return true or false. It it returns false then this
	// Emitter will not be used within the Map or Reduce methods.
	Where() bool
}

// Reducer implements the Sum method. It is used within the reduce stage to perform
// summing of sequential matching keys and their values
type Reducer interface {
	Emitter
	// Provides the key to use for grouping the sum operations
	Key() string
	// Sum implements logic to Sum together two copies of this Emitter.
	// The underlying type of the receiver and the argument will always be
	// identical.
	Sum(emitter ...Emitter)
}

// Finalize implements a Finalize method. During the reduce stage, if a type
// implements the Finalizer interface Finalize is called once for each reduced Key.
type Finalizer interface {
	Reducer
	Finalize(w io.Writer) string
}
