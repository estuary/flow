package schemabuilder

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os/exec"
)

// ProgramName is the name of schema builder binary built from rust.
const ProgramName = "schema-builder"

// DateSpec configures a date field in elastic search schema.
// https://www.elastic.co/guide/en/elasticsearch/reference/current/mapping-date-format.html
type DateSpec struct {
	Format string `json:"format"`
}

// KeywordSpec configures a keyword field for elastic search schema.
type KeywordSpec struct {
	//https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html
	IgnoreAbove int `json:"ignore_above"`
}

// TextSpec configures a text field for elastic search schema.
type TextSpec struct {
	// Whether or not to specify the field as text/keyword dual field.
	DualKeyword bool `json:"dual_keyword"`

	// https://www.elastic.co/guide/en/elasticsearch/reference/current/ignore-above.html
	// effective only if DualKeyword is enabled.
	KeywordIgnoreAbove int `json:"keyword_ignore_above"`
}

// ElasticFieldType specifies the type to override the field with.
type ElasticFieldType struct {
	// A snake_case string corresponding to a enum type of ESBasicType
	// defined in src/elastic_search_data_types.rs
	FieldType string `json:"field_type"`
	// Effective if FieldType is "date"
	DateSpec DateSpec `json:"date_spec,omitempty"`
	// Effective if FieldType is "keyword"
	KeywordSpec KeywordSpec `json:"keyword_spec,omitempty"`
	// Effective if FieldType is "text"
	TextSpec TextSpec `json:"text_spec,omitempty"`
}

// MarshalJSON provides customized marshalJSON of ElasticFieldType
func (e ElasticFieldType) MarshalJSON() ([]byte, error) {
	var m = make(map[string]interface{})
	var spec interface{}
	switch e.FieldType {
	case "date":
		spec = e.DateSpec
	case "keyword":
		spec = e.KeywordSpec
	case "text":
		spec = e.TextSpec
	default:
		spec = nil
	}

	m["type"] = e.FieldType
	if spec != nil {
		if specJson, err := json.Marshal(spec); err != nil {
			return nil, err
		} else if err = json.Unmarshal(specJson, &m); err != nil {
			return nil, err
		}
	}

	return json.Marshal(m)
}

// FieldOverride specifies which field in the resulting elastic search schema
// and how it is overridden.
type FieldOverride struct {
	// A '/'-delimitated json pointer to the location of the overridden field.
	Pointer string `json:"pointer"`
	// The overriding elastic search data type of the field.
	EsType ElasticFieldType `json:"es_type"`
}

// Input provides the input data for schema builder.
type Input struct {
	SchemaJSON []byte
	overrides  []FieldOverride
}

// MarshalJSON provides customized marshalJSON of Input
func (s Input) MarshalJSON() ([]byte, error) {
	var output = struct {
		SchemaJSONBase64 string          `json:"schema_json_base64"`
		Overrides        []FieldOverride `json:"overrides"`
	}{
		SchemaJSONBase64: base64.StdEncoding.EncodeToString(s.SchemaJSON),
		Overrides:        s.overrides,
	}
	return json.Marshal(output)
}

// RunSchemaBuilder is a wrapper in GO around rust schema-builder.
func RunSchemaBuilder(
	schemaJSON json.RawMessage,
	overrides []FieldOverride,
) ([]byte, error) {
	var cmd = exec.Command(ProgramName)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	stdin, err := cmd.StdinPipe()
	if err != nil {
		return nil, fmt.Errorf("getting stdin pipeline: %w", err)
	}

	input, err := json.Marshal(Input{
		SchemaJSON: schemaJSON,
		overrides:  overrides,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal input: %w", err)
	}

	go func() {
		defer stdin.Close()
		stdin.Write(input)
	}()

	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("fetching output: %w. With stderr: %s", err, stderr.String())
	}
	return out, nil
}
