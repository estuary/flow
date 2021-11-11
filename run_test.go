package schemabuilder

import (
	"encoding/json"
	"testing"

	"github.com/bradleyjkemp/cupaloy"
	"github.com/stretchr/testify/require"
)

const schemaJSON = `
	  { "$defs": {
                "__flowInline1":{
                    "$defs":{
                        "anAnchor": {
                            "$anchor": "AnAnchor",
                            "properties": {
                                "one":{"type": "string"},
                                "two":{"type": "integer"}
                            },
                            "required":["one"],
                            "type":"object"
                        }
                    },
                    "$id": "test://example/int-string.schema",
                    "properties": {
                        "bit": { "type": "boolean" },
                        "int": { "type": "integer" },
                        "str": { "type": "string" }
                    },
                    "required": ["int", "str", "bit"],
                    "type": "object"
                }
            },
            "$id": "test://example/int-string-len.schema",
            "$ref": "test://example/int-string.schema",
            "properties": {
                "arr":{
                    "items":{"$ref": "int-string.schema#AnAnchor"},
                    "type":"array"
                },
                "len":{"type": "integer"}
            },
            "required":["len"]
        }
`

func TestRunSchemaBuilder_NoOverrides(t *testing.T) {
	result, err := RunSchemaBuilder(json.RawMessage(schemaJSON), []FieldOverride{})
	require.NoError(t, err)

	cupaloy.SnapshotT(t, result)
}

func TestRunSchemaBuilder_WithOverrides(t *testing.T) {
	var dateFieldOverride FieldOverride
	require.NoError(t, json.Unmarshal(
		[]byte(`{"pointer": "/arr/one", "es_type": {"field_type": "date", "date_spec": {"format": "test_format"}}}`),
		&dateFieldOverride),
	)

	var keywordFieldOverride FieldOverride
	require.NoError(t, json.Unmarshal(
		[]byte(`{"pointer": "/bit", "es_type": {"field_type": "text", "text_spec": {"dual_keyword": true, "keyword_ignore_above": 500}}}`),
		&keywordFieldOverride),
	)

	var textFieldOverride FieldOverride
	require.NoError(t, json.Unmarshal(
		[]byte(`{"pointer": "/int", "es_type": {"field_type": "keyword", "keyword_spec": {"ignore_above": 400}}}`),
		&textFieldOverride),
	)

	var overrides = []FieldOverride{dateFieldOverride, keywordFieldOverride, textFieldOverride}

	result, e := RunSchemaBuilder(json.RawMessage(schemaJSON), overrides)
	require.NoError(t, e)
	cupaloy.SnapshotT(t, string(result))
}

func TestRunSchemaBuilder_Errors(t *testing.T) {
	_, e := RunSchemaBuilder(json.RawMessage("{}"), []FieldOverride{})
	require.Contains(t, e.Error(), "a valid $id field in the input json schema is missing.")

	var corruptedSchema = "corrupted schema"
	_, e = RunSchemaBuilder(json.RawMessage(corruptedSchema), []FieldOverride{})
	require.Contains(t, e.Error(), "Failed generating elastic search schema based on input")

	var badOverrides = []FieldOverride{
		{Pointer: "/arr/nonexisting_field", EsType: ElasticFieldType{FieldType: "text"}},
	}
	_, e = RunSchemaBuilder(json.RawMessage(schemaJSON), badOverrides)
	require.Contains(t, e.Error(), "pointer of a non-existing field")
}
