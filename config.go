package parser

import (
	"encoding/json"
	"os"
)

type JsonPointer string

// Config represents the parser configuration json. It matches the Rust type used by the parser.
// Eventually, it might be nice to generate this definition from the json schema output from the
// parser.
type Config struct {
	AddRecordOffset       string                      `json:"addRecordOffset,omitempty"`
	AddValues             map[JsonPointer]interface{} `json:"addValues,omitempty"`
	Format                string                      `json:"format,omitempty"`
	Filename              string                      `json:"filename,omitempty"`
	ContentType           string                      `json:"contentType,omitempty"`
	ContentEncoding       string                      `json:"contentEncoding,omitempty"`
	Projections           map[string]JsonPointer      `json:"projections,omitempty"`
	Schema                json.RawMessage             `json:"schema,omitempty"`
	FileExtensionMappings map[string]string           `json:"fileExtensionMappings,omitempty"`
}

func (c *Config) Copy() Config {
	var newAddValues = make(map[JsonPointer]interface{})
	for k, v := range c.AddValues {
		newAddValues[k] = v
	}
	var newProjections = make(map[string]JsonPointer)
	for k, v := range c.Projections {
		newProjections[k] = v
	}
	var newFileMappings = make(map[string]string)
	for k, v := range c.FileExtensionMappings {
		newFileMappings[k] = v
	}

	return Config{
		AddRecordOffset: c.AddRecordOffset,
		AddValues:       newAddValues,
		Format:          c.Format,
		Filename:        c.Filename,
		ContentType:     c.ContentType,
		ContentEncoding: c.ContentEncoding,
		Projections:     newProjections,
		// TODO: Figure out a non-heinous way to copy an interface{}
		Schema:                c.Schema,
		FileExtensionMappings: newFileMappings,
	}
}

// WriteTo writes the Config as JSON to a file at the given path. The file will be created if it
// does not exist, and will be overwritten if it does exist.
func (c *Config) WriteTo(path string) error {
	var file, err = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0640)
	if err != nil {
		return err
	}
	return c.WriteToFile(file)
}

// WriteToFile writes the config to the given open file, which must be writable.
func (c *Config) WriteToFile(file *os.File) error {
	var err = json.NewEncoder(file).Encode(c)
	var ioerr = file.Close()
	if err == nil {
		return ioerr
	}
	return err
}
