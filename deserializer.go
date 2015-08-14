package pipeline

import "io"

// DeserializerFunc is a function that accepts an io.Reader and returns a
// Deserializer interface.
type DeserializerFunc func(io.Reader) Deserializer

// Deserializer is an interface for deserializing the input data into Emitter
// interfaces which can then be used within other pipeline methods.
type Deserializer interface {
	// HasNext should return true if there is data available
	HasNext() bool
	// Next should return the next available Emitter interface
	Next() Emitter
	// Error should return the last error to occur
	Error() error
}

// deserializeInput reads data from r and deserializes it using the Deserializer provided by
// calling d. It returns a channel of Emitter interfaces.
func deserializeInput(d DeserializerFunc, r io.Reader) chan Emitter {

	// Call the DeserializerFunc to return a new Deserializer
	deserializer := d(r)

	// Create a channel of Emitter interfaces
	ch := make(chan Emitter)

	go func() {
		// Whilst the deserializers HasNext method returns true
		for deserializer.HasNext() {
			// Get the next Emitter and send to the channel
			ch <- deserializer.Next()
		}
		// Close the channel on completion
		close(ch)
		// If an error has occurred that is not due to reaching EOF then panic
		// TODO: Find a good way to handle this other than panicking.
		if deserializer.Error() != nil && deserializer.Error() != io.EOF {
			panic(deserializer.Error())
		}
	}()
	// Return the channel
	return ch

}
