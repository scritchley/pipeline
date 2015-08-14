package pipeline

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"reflect"
)

type TSVDeserializer struct {
	constructor func() Emitter
	scanner     *bufio.Scanner
	next        func() Emitter
}

func (m *TSVDeserializer) HasNext() bool {
	return m.scanner.Scan()
}

func (m *TSVDeserializer) Error() error {
	return m.scanner.Err()
}

func (m *TSVDeserializer) Next() Emitter {
	return m.next()
}

func NewTSVDeserializer(constructor func() Emitter) DeserializerFunc {

	return func(r io.Reader) Deserializer {

		// Scanner splits into lines
		scanner := bufio.NewScanner(r)

		// Get the type returned by the constructor
		ty := reflect.ValueOf(constructor()).Elem()

		// A slice to store setters
		setters := make([]func(cols [][]byte), 0)

		var recurseStructFields func(ty reflect.Value, index []int)

		recurseStructFields = func(ty reflect.Value, index []int) {

			field := ty.FieldByIndex(index)

			// Structs should be flattened
			if field.Kind() == reflect.Struct {

				for i := 0; i < field.NumField(); i++ {

					nestedIndex := append(index, i)

					recurseStructFields(ty, nestedIndex)

				}

				return

			}

			// Slices should be turned into JSON
			if field.Kind() == reflect.Slice {

				setters = append(setters, func(cols [][]byte) {

					primaryIndex := index[0]

					column := cols[primaryIndex]

					fmt.Println(reflect.SliceOf(field.Type()).Kind(), string(column))

				})

				return

			}

			// Values should be flattened
			setters = append(setters, func(cols [][]byte) {

				primaryIndex := index[0]

				column := cols[primaryIndex]

				fmt.Println(field.Kind(), string(column))

			})

		}

		for i := 0; i < ty.NumField(); i++ {

			index := []int{i}

			recurseStructFields(ty, index)

		}

		// Unmarshal uses reflection to unmarshal into the struct using the correct column.
		// Currently this only supports flat structs
		unmarshalEmitter := func(cols [][]byte, e Emitter) {

			for _, s := range setters {
				s(cols)
			}

		}

		// Next gets the next slice of bytes from the scanner and unmarshals
		// it into the Emitter provided by the constructor func.
		nextFunc := func() Emitter {

			// Get next row
			byt := scanner.Bytes()

			// Split the row into columns
			cols := bytes.Split(byt, []byte("\t"))

			// Create a new Emitter from the constructor
			e := constructor()

			unmarshalEmitter(cols, e)

			return e

		}

		return &TSVDeserializer{
			constructor: constructor,
			scanner:     scanner,
			next:        nextFunc,
		}

	}

}
