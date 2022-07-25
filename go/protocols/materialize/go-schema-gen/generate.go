package schemagen

import (
	"reflect"

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
	fixSchemaFlagBools(schema, "secret", "advanced", "multiline")
	return schema
}

func fixSchemaFlagBools(t *jsonschema.Schema, flagKeys ...string) {
	if t.Properties != nil {
		for _, key := range t.Properties.Keys() {
			if p, ok := t.Properties.Get(key); ok {
				if p, ok := p.(*jsonschema.Schema); ok {
					fixSchemaFlagBools(p, flagKeys...)
				}
			}
		}
	}
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
