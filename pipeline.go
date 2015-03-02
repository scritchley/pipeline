package pipeline

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"os"
	"sort"
)

const (
	KB int = 1024
	MB int = 1024 * KB
)

var DefaultKeyFieldDelimiter = []byte("\t")
var NewLine = []byte("\n")

type MapReducer struct {
	w io.Writer
	r io.Reader
	d Deserializer
	k []byte
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

// New returns a pointer to a new MapReducer with the default `\t` key field delimiter
func New(r io.Reader, w io.Writer) *MapReducer {
	return &MapReducer{
		r: r,
		w: w,
		k: DefaultKeyFieldDelimiter,
	}
}

// Deserializer sets the Deserializer for the MapReducer to use
// given Deserializer func
func (m *MapReducer) Deserializer(d Deserializer) {
	m.d = d
}

// Formatter sets the Deserializer for the MapReducer
func (m *MapReducer) SetDelimiter(s string) {
	m.k = []byte(s)
}

// Emit writes to given key and value to the output writer.
// If an empty key is provided it will just write the value.
func (m *MapReducer) Emit(key string, value string) {
	// If there is a key provided then include it along with the delimiter
	m.w.Write([]byte(key))   // Write key
	m.w.Write(m.k)           // Write delimiter
	m.w.Write([]byte(value)) // Write value
	m.w.Write(NewLine)       // Add new line
}

// Map ranges over a channel of Emitters provided by the deserializer
// and calls fn for each.
func (m *MapReducer) Map() {
	// Return a channel from the formatter
	ch := m.deserializeInput(m.d)
	// Iterate over the channel and call fn
	for r := range ch {
		// Continue if Filter method returns false
		if !r.Filter() {
			continue
		}
		m.Emit(r.Key(), r.Value())
	}
}

// Reduce ranges over a channel of Emitters provided by the deserializer
// and calls reduce passing in the previous and current Emitters along
// with the provided func.
func (m *MapReducer) Reduce() {
	// Return a channel from the formatter
	ch := m.deserializeInput(m.d)
	var prev Reducer
	for r := range ch {
		// Continue if Filter method returns false
		if !r.Filter() {
			continue
		}
		curr, ok := r.(Reducer)
		if !ok {
			continue
		}
		prev = m.reduce(prev, curr)
	}
	if prev != nil {
		m.finalize(prev)
	}
}

// reduce performs the reducer logic utilised in Reduce. It takes the
// previous and current Emitter, checks whether they contain matching keys
// and if so performs a Sum operation. If the keys do not match it calls
// fn on the Emitter. It returns the current Emitter to be used as the
// previous Emitter on the next iteration of the Reduce func.
func (m *MapReducer) reduce(prev Reducer, curr Reducer) Reducer {
	// If nil, then first run, set r to previous and return
	if prev == nil {
		return curr
	}
	// If keys match then perform sum operation and return
	if prev.Key() == curr.Key() {
		prev.Sum(curr)
		return prev
	}
	m.finalize(prev)
	return curr
}

// finalize checks whether the given Reducer implements a
// Finalizer interface and if so calls the Finalize method.
func (m *MapReducer) finalize(r Reducer) {
	f, ok := r.(Finalizer)
	if !ok {
		// If keys do not match call fn for previous and set r to previous
		m.Emit(r.(Emitter).Key(), r.(Emitter).Value())
		return
	}
	// If the Emitter implements the Finalizer interface
	// then call the Finalize method
	m.Emit(r.(Emitter).Key(), f.Finalize())
}

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

type Reducer interface {
	Emitter
	// Sum implements logic to Sum together two copies of this Emitter.
	// The underlying type of the receiver and the argument will always be
	// identical.
	Sum(emitter Emitter)
}

// Finalizer implements a Finalize method. During the reduce stage, if a type
// implements the Finalizer interface Finalize is called once for each reduced Key.
type Finalizer interface {
	Reducer
	Finalize() string
}

// Deserializer should take an empty interface and return an Emitter.
// It is used within the Map and Reduce functions to deserialize incoming
// data into an type that implements the Emitter interface.
type Deserializer func([]byte) Emitter

// deserializeInput returns a chan of Emitter when given a Deserializer
// This implementation is designed to work with Hadoop streaming and a JSON
// input format.
func (m *MapReducer) deserializeInput(d Deserializer) chan Emitter {
	// Create a channel of Emitter to send incoming
	// deserialized rows to the MapReducer
	ch := make(chan Emitter)
	go func(d Deserializer, s *bufio.Scanner) {
		for s.Scan() {
			ch <- d(s.Bytes())
		}
		if err := s.Err(); err != nil {
			fmt.Fprintln(os.Stderr, "reading input:", err)
		}
		close(ch)
	}(d, bufio.NewScanner(m.r))
	return ch
}

// HadoopStreamingJSON returns a Deserializer that parses a byte slice and
// unmarshals it to the Emitter type provided by calling fn.
func HadoopStreamingJSON(fn func() Emitter) Deserializer {
	return func(b []byte) Emitter {
		// Find a tab delimiter (the hadoop default key field delimiter)
		delim := bytes.Index(b, DefaultKeyFieldDelimiter)
		// Create a new row, could look at implementing sync.Pool for this
		s := fn()
		// If the delimiter is present separate into key and value portions
		if delim != -1 {
			b = b[delim+1:]
		}
		json.Unmarshal(b, &s)
		return s
	}
}

// Emitters is a slice of Emitter interfaces that implements the sort.Interface
type Emitters []Emitter

func (e Emitters) Len() int           { return len(e) }
func (e Emitters) Swap(i, j int)      { e[i], e[j] = e[j], e[i] }
func (e Emitters) Less(i, j int) bool { return e[i].Key() < e[j].Key() }

// Sort sorts an unsorted input and emits it to the output writer. It currently
// only supports datasets that fit in memory.
// TODO: Improve sorting algo.
func (m *MapReducer) Sort() {
	// Return a channel from the formatter
	ch := m.deserializeInput(m.d)
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
		m.Emit(o.(Emitter).Key(), o.Value())
	}
}
