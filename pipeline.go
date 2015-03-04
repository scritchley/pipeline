package pipeline

import (
	"fmt"
	"hash/fnv"
	"io"
)

const (
	KB int = 1024
	MB int = 1024 * KB
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

// emit writes to given key and value to a writer.
// If an empty key is provided it will just write the value.
func emit(w io.Writer, key string, value string) {
	// If there is a key provided then include it along with the delimiter
	w.Write([]byte(key))              // Write key
	w.Write(DefaultKeyFieldDelimiter) // Write delimiter
	w.Write([]byte(value))            // Write value
	w.Write(NewLine)                  // Add new line
}

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
	// Key returns a string for the given Emitter, used for
	// determining when to perform the Sum operation within the Reducer
	Key() string
	// Value should return a string to be emitted as a value during the Map phase
	Value() string
	// Filter should return true or false. It it returns false then this
	// Emitter will not be used within the Map or Reduce methods.
	Filter() bool
}

// Summer implements the Sum method. It is used within the reduce stage to perform
// summing of sequential matching keys and their values
type Summer interface {
	Emitter
	// Sum implements logic to Sum together two copies of this Emitter.
	// The underlying type of the receiver and the argument will always be
	// identical.
	Sum(emitter Emitter)
}

// Finalizer implements a Finalize method. During the reduce stage, if a type
// implements the Finalizer interface Finalize is called once for each reduced Key.
type Finalizer interface {
	Summer
	Finalize() string
}
