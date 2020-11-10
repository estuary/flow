package ingest

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/estuary/flow/go/flow"
	pf "github.com/estuary/flow/go/protocol"
)

type fieldMapping struct {
	parse         []func(string) (interface{}, error)
	field         string
	ptr           string
	isOptional    bool
	possibleTypes []string
}

func (self *fieldMapping) extractValue(input string) (interface{}, error) {
	// Is this value allowed to be empty? If it is, then we'll let the parsers determine the proper
	// value to return.
	if len(input) == 0 && !self.isOptional {
		return nil, fmt.Errorf("value cannot be null")
	}
	var val interface{}
	var lastErr error
	for _, parser := range self.parse {
		val, lastErr = parser(input)
		if lastErr == nil {
			return val, nil
		}
	}
	return nil, fmt.Errorf("failed to parse '%v' (of column: %v) into %v: %w", input, self.field, self.possibleTypes, lastErr)
}

func hasType(projection *pf.Projection, typeName string) bool {
	for _, ty := range projection.Inference.Types {
		if ty == typeName {
			return true
		}
	}
	return false
}

// We traverse these types in a fairly specific order, which determines the order in which parsers
// are applied in the case that a field may have multiple types. The first type that parses
// successfully will be used.
var orderedTypes = []string{"integer", "number", "boolean", "null", "string", "object", "array"}

func newFieldMapping(projection *pf.Projection) fieldMapping {
	// Whether it's valid for this field to have an empty value as input. This will be true if the
	// allowable types include either null or string
	var isOptional = false
	var parse []func(string) (interface{}, error)
	for _, typeName := range orderedTypes {
		if !hasType(projection, typeName) {
			continue
		}
		switch typeName {
		case "null":
			// empty input will be interpreted as null. This will take precedence over empty strings
			isOptional = true
			parse = append(parse, parseNull)
		case "string":
			// empty input will be interpreted as an empty string
			isOptional = true
			parse = append(parse, parseString)
		case "boolean":
			parse = append(parse, parseBoolean)
		case "integer":
			parse = append(parse, parseInt)
		case "number":
			parse = append(parse, parseNumber)
		case "object":
			parse = append(parse, parseObject)
		case "array":
			parse = append(parse, parseArray)
		}
	}
	return fieldMapping{
		field:         projection.Field,
		ptr:           projection.Ptr,
		parse:         parse,
		isOptional:    isOptional,
		possibleTypes: projection.Inference.Types,
	}
}

type wsCsvIngester struct {
	buffer             *bytes.Buffer
	csvReader          *csv.Reader
	projections        []fieldMapping
	pointers           []flow.Pointer
	lastMustExistIndex int
}

// First frame is headers, subsequent frames are documents.
func (self *wsCsvIngester) onHeader(collection *pf.CollectionSpec) error {
	var headers, err = self.csvReader.Read()
	if err != nil {
		return err
	}

	var columnPointers = make(map[string]bool)
	for i, header := range headers {
		var projection = pf.GetProjectionByField(header, collection.Projections)
		if projection == nil {
			return fmt.Errorf("collection %q has no projection %q", collection.Name, header)
		}
		self.projections = append(self.projections, newFieldMapping(projection))

		var ptr, err = flow.NewPointer(projection.Ptr)
		if err != nil {
			panic(err)
		}
		self.pointers = append(self.pointers, ptr)

		if projection.Inference.MustExist {
			self.lastMustExistIndex = i
		}
		columnPointers[projection.Ptr] = true
	}

	// Go through the set of projections on this collection and validate that the headers include a
	// minimum viable subset of fields. This means any location that's required to exist by the
	// schema, and anything that's used as a collection key. Technically, all collection keys are
	// already required to exist, though, so there's no need for an explicit check for those here.
	// This is technically not necessary, since the documents must all individually pass validation,
	// but doing the check here allows us to fail fast (in case there's a delay between receiving
	// headers and the rest of the data), and with an error message that's hopefully more clear and
	// explicit than the validation error.
	for _, projection := range collection.Projections {
		if projection.Inference.MustExist && !columnPointers[projection.Ptr] {
			return fmt.Errorf("Header does not include any field that maps to the location: '%s', which is required to exist by the collection schema", projection.Ptr)
		}
	}
	return nil
}

func (self *wsCsvIngester) onFrame(collection *pf.CollectionSpec, addCh chan<- ingestAdd) error {
	if len(self.projections) == 0 {
		if err := self.onHeader(collection); err != nil {
			return err
		}
	}

	for self.buffer.Len() != 0 {
		var records, err = self.csvReader.Read()
		if err != nil {
			return err
		} else if lr, lp := len(records), len(self.projections); lr > lp {
			return fmt.Errorf("row has %d columns, but header had only %d", lr, lp)
		} else if lr <= self.lastMustExistIndex {
			return fmt.Errorf("row omits column %d ('%v'), which must exist", self.lastMustExistIndex, self.projections[self.lastMustExistIndex].field)
		}

		// Doc we'll build up from parsed projections.
		var doc interface{}

		for c, record := range records {
			var mapping = self.projections[c]
			// We know this can't be undefined since there's a row for it
			happyValueHome, err := self.pointers[c].Create(&doc)
			if err != nil {
				return fmt.Errorf("failed to query or create document location %q: %w", mapping.ptr, err)
			}

			*happyValueHome, err = mapping.extractValue(record)
			if err != nil {
				return err
			}
		}

		docBytes, err := json.Marshal(doc)
		if err != nil {
			panic(err) // Marshal cannot fail.
		}

		addCh <- ingestAdd{
			collection: collection.Name,
			doc:        json.RawMessage(docBytes),
		}
	}
	return nil
}

func parseInt(input string) (interface{}, error) {
	if value, err := strconv.ParseUint(input, 10, 64); err == nil {
		return value, nil
	}
	value, err := strconv.ParseInt(input, 10, 64)
	return value, err
}

func parseNumber(input string) (interface{}, error) {
	return strconv.ParseFloat(input, 64)
}

func parseBoolean(input string) (interface{}, error) {
	return strconv.ParseBool(input)
}

func parseObject(input string) (interface{}, error) {
	return nil, fmt.Errorf("unspported type 'object'")
}

func parseArray(input string) (interface{}, error) {
	return nil, fmt.Errorf("unspported type 'array'")
}

func parseString(input string) (interface{}, error) {
	// Empty strings are totally allowed
	return input, nil
}

func parseNull(input string) (interface{}, error) {
	if len(input) == 0 {
		return nil, nil
	} else {
		return nil, fmt.Errorf("expected an empty value")
	}
}

func nullValueError(input string) (interface{}, error) {
	if len(input) == 0 {
		return nil, fmt.Errorf("value cannot be null")
	} else {
		panic("non-null value passed to nullValueError")
	}
}
