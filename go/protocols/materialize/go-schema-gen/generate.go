package schemagen

import (
	"reflect"
	"strconv"

	"github.com/invopop/jsonschema"
)

/// WARNING: This file is copied from github.com/estuary/connectors/go-schema-gen/generate.go.
///
/// We have a few schema generation fixes that we need to be applied to our
/// connector schemas. However, sql-based materialization connectors delegate to
/// the sql/driver package to generate it schema. Until we move sql/driver out
/// of the flow repo (planned), we'll need to copy this behavior here so that we
/// can properly mark schema fields as `secret`/`advanced`.

func GenerateSchema(title string, configObject interface{}) *jsonschema.Schema {
	// By default, the library generates schemas with a top-level $ref that references a definition.
	// That breaks UI code that tries to generate forms from the schemas, and is just weird and
	// silly anyway. While we're at it, we just disable references altogether, since they tend to
	// hurt readability more than they help for these schemas.
	var reflector = jsonschema.Reflector{
		ExpandedStruct: true,
		DoNotReference: true,
	}
	var schema = reflector.ReflectFromType(reflect.TypeOf(configObject))
	schema.AdditionalProperties = nil // Unset means additional properties are permitted on the root object, as they should be
	schema.Definitions = nil          // Since no references are used, these definitions are just noise
	schema.Title = title
	walkSchema(
		schema,
		fixSchemaFlagBools(schema, "secret", "advanced", "multiline"),
		fixSchemaOrderingStrings,
	)

	return schema
}

// walkSchema invokes visit on every property of the root schema, and then traverses each of these
// sub-schemas recursively. The visit function should modify the provided schema in-place to
// accomplish the desired transformation.
func walkSchema(root *jsonschema.Schema, visits ...func(t *jsonschema.Schema)) {
	if root.Properties != nil {
		for _, key := range root.Properties.Keys() {
			if p, ok := root.Properties.Get(key); ok {
				if p, ok := p.(*jsonschema.Schema); ok {
					for _, visit := range visits {
						visit(p)
					}

					walkSchema(p, visits...)
				}
			}
		}
	}
}

func fixSchemaFlagBools(t *jsonschema.Schema, flagKeys ...string) func(t *jsonschema.Schema) {
	return func(t *jsonschema.Schema) {
		for key, val := range t.Extras {
			for _, flag := range flagKeys {
				if key != flag {
					continue
				} else if val == "true" {
					t.Extras[key] = true
				} else if val == "false" {
					t.Extras[key] = false
				}
			}
		}
	}
}

func fixSchemaOrderingStrings(t *jsonschema.Schema) {
	for key, val := range t.Extras {
		if key == "order" {
			if str, ok := val.(string); ok {
				converted, err := strconv.Atoi(str)
				if err != nil {
					// Don't try to convert strings that don't look like integers.
					continue
				}
				t.Extras[key] = converted
			}
		}
	}
}
