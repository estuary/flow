package flow

import (
	"fmt"
	"strconv"

	"github.com/go-openapi/jsonpointer"
)

// Pointer is a parsed JSON Pointer.
type Pointer struct {
	jsonpointer.Pointer
	Tokens []string
}

// NewPointer parses a Pointer from a JSON Pointer string.
func NewPointer(s string) (Pointer, error) {
	var ptr, err = jsonpointer.New(s)
	if err != nil {
		return Pointer{}, err
	}
	return Pointer{
		Pointer: ptr,
		Tokens:  ptr.DecodedTokens(),
	}, nil
}

// Create or query a mutable existing value at the pointer location within the document,
// recursively creating the location if it doesn't exist. Existing parent locations
// which don't yet exist are instantiated as an object or array, depending on the type of
// token at that location (integer, "-", or property name). An existing array is
// extended with nulls as required to instantiate a specified index.
// Returns a mutable *interface{} at the pointed location, or an error if the document
// structure is incompatible with the pointer (eg, because a parent location is
// a scalar type, or attempts to index an array by-property).
func (p Pointer) Create(doc *interface{}) (*interface{}, error) {
	var next = doc
	var child *interface{}

	for _, token := range p.Tokens {
		var index, indexErr = strconv.Atoi(token)

		if *next == nil {
			if indexErr != nil && token != "-" {
				*next = make(map[string]*interface{})
			} else {
				*next = make([]*interface{}, 0)
			}
		}

		switch vv := (*next).(type) {
		case map[string]*interface{}:
			if child = vv[token]; child == nil {
				child = new(interface{})
				vv[token] = child
			}

		case []*interface{}:
			if token == "-" {
				child = new(interface{})
				*next = append(vv, child)
			} else if indexErr == nil {
				for len(vv) <= index {
					vv = append(vv, nil)
				}
				*next = vv // Update with extended slice.

				if child = vv[index]; child == nil {
					child = new(interface{})
					vv[index] = child
				}
			} else {
				return nil, fmt.Errorf("expected array, not %v", *next)
			}
		default:
			return nil, fmt.Errorf("expected object or array, not %v", *next)
		}
		next = child
	}
	return next, nil
}
