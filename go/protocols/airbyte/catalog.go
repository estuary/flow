package airbyte

import (
	"encoding/json"
	"fmt"
)

type SyncMode string

const (
	SyncModeIncremental SyncMode = "incremental"
	SyncModeFullRefresh SyncMode = "full_refresh"
)

var AllSyncModes = []SyncMode{SyncModeIncremental, SyncModeFullRefresh}

type Stream struct {
	Name                    string          `json:"name"`
	JSONSchema              json.RawMessage `json:"json_schema"`
	SupportedSyncModes      []SyncMode      `json:"supported_sync_modes"`
	SourceDefinedCursor     bool            `json:"source_defined_cursor,omitempty"`
	DefaultCursorField      []string        `json:"default_cursor_field,omitempty"`
	SourceDefinedPrimaryKey [][]string      `json:"source_defined_primary_key,omitempty"`
	Namespace               string          `json:"namespace,omitempty"`
}

func (s *Stream) Validate() error {
	if len(s.SupportedSyncModes) == 0 {
		return fmt.Errorf("stream must have at least one supported_sync_modes")
	}
	return nil
}

type DestinationSyncMode string

const (
	DestinationSyncModeAppend      DestinationSyncMode = "append"
	DestinationSyncModeOverwrite   DestinationSyncMode = "overwrite"
	DestinationSyncModeAppendDedup DestinationSyncMode = "append_dedup"
)

var AllDestinationSyncModes = []DestinationSyncMode{
	DestinationSyncModeAppend,
	DestinationSyncModeOverwrite,
	DestinationSyncModeAppendDedup,
}

type ConfiguredStream struct {
	Stream              Stream              `json:"stream"`
	SyncMode            SyncMode            `json:"sync_mode"`
	DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode"`
	CursorField         []string            `json:"cursor_field,omitempty"`
	PrimaryKey          [][]string          `json:"primary_key,omitempty"`
	Projections         map[string]string   `json:"estuary.dev/projections"`
}

// This impl exists solely so that we can allow deserializing either the namespaced or
// non-namepsaced version of Projections, for the purpose of compatibility.
func (s *ConfiguredStream) UnmarshalJSON(b []byte) error {
	var tmp = struct {
		Stream              Stream              `json:"stream"`
		SyncMode            SyncMode            `json:"sync_mode"`
		DestinationSyncMode DestinationSyncMode `json:"destination_sync_mode"`
		CursorField         []string            `json:"cursor_field,omitempty"`
		PrimaryKey          [][]string          `json:"primary_key,omitempty"`
		NSProjections       map[string]string   `json:"estuary.dev/projections"`
		Projections         map[string]string   `json:"projections"`
	}{}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	*s = ConfiguredStream{
		Stream:              tmp.Stream,
		SyncMode:            tmp.SyncMode,
		DestinationSyncMode: tmp.DestinationSyncMode,
		CursorField:         tmp.CursorField,
		PrimaryKey:          tmp.PrimaryKey,
		Projections:         tmp.NSProjections,
	}
	if len(s.Projections) == 0 {
		s.Projections = tmp.Projections
	}
	return nil
}

func (c *ConfiguredStream) Validate() error {
	var err = c.Stream.Validate()
	if err != nil {
		return fmt.Errorf("stream invalid: %w", err)
	}
	var syncTypeValid = false
	for _, m := range c.Stream.SupportedSyncModes {
		if m == c.SyncMode {
			syncTypeValid = true
		}
	}
	if !syncTypeValid {
		return fmt.Errorf("unsupported syncMode: %s", c.SyncMode)
	}
	return nil
}

type Catalog struct {
	Streams []Stream `json:"streams"`
}

type ConfiguredCatalog struct {
	Streams []ConfiguredStream `json:"streams"`
	Tail    bool               `json:"estuary.dev/tail"`
	Range   Range              `json:"estuary.dev/range"`
}

// This impl exists solely so that we can accept either the namespaced or non-namespaced identifiers
// for tail and range, for the purpose of compatibility.
func (c *ConfiguredCatalog) UnmarshalJSON(b []byte) error {
	var tmp = struct {
		Streams []ConfiguredStream `json:"streams"`
		NSTail  *bool              `json:"estuary.dev/tail"`
		Tail    *bool              `json:"tail"`
		NSRange *Range             `json:"estuary.dev/range"`
		Range   *Range             `json:"range"`
	}{}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}
	var tail bool
	if tmp.NSTail != nil {
		tail = *tmp.NSTail
	} else if tmp.Tail != nil {
		tail = *tmp.Tail
	}
	var r Range
	if tmp.NSRange != nil {
		r = *tmp.NSRange
	} else if tmp.Range != nil {
		r = *tmp.Range
	}
	*c = ConfiguredCatalog{
		Streams: tmp.Streams,
		Tail:    tail,
		Range:   r,
	}
	return nil
}

func (c *ConfiguredCatalog) Validate() error {
	if len(c.Streams) == 0 {
		return fmt.Errorf("catalog must have at least one stream")
	}
	for i, s := range c.Streams {
		if err := s.Validate(); err != nil {
			return fmt.Errorf("Streams[%d]: %w", i, err)
		}
	}
	if err := c.Range.Validate(); err != nil {
		return fmt.Errorf("Range: %w", err)
	}
	return nil
}

type Status string

const (
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
)

type ConnectionStatus struct {
	Status  Status `json:"status"`
	Message string `json:"message"`
}

type Record struct {
	Stream    string          `json:"stream"`
	Data      json.RawMessage `json:"data"`
	EmittedAt int64           `json:"emitted_at"`
	Namespace string          `json:"namespace,omitempty"`
}

type LogLevel string

const (
	LogLevelTrace LogLevel = "TRACE"
	LogLevelDebug LogLevel = "DEBUG"
	LogLevelInfo  LogLevel = "INFO"
	LogLevelWarn  LogLevel = "WARN"
	LogLevelError LogLevel = "ERROR"
	LogLevelFatal LogLevel = "FATAL"
)

type Log struct {
	Level   LogLevel `json:"level"`
	Message string   `json:"message"`
}

type State struct {
	// Data is the actual state associated with the ingestion. This must be a JSON _Object_ in order
	// to comply with the airbyte specification.
	Data json.RawMessage `json:"data"`
	// Merge indicates that Data is an RFC 7396 JSON Merge Patch, and should
	// be be reduced into the previous state accordingly.
	Merge bool `json:"estuary.dev/merge,omitempty"`
}

func (s *State) UnmarshalJSON(b []byte) error {
	var tmp = struct {
		Data    json.RawMessage `json:"data"`
		NSMerge bool            `json:"estuary.dev/merge"`
		Merge   bool            `json:"merge"`
	}{}
	if err := json.Unmarshal(b, &tmp); err != nil {
		return err
	}

	s.Data = tmp.Data
	s.Merge = tmp.NSMerge || tmp.Merge

	return nil
}

type Spec struct {
	DocumentationURL        string          `json:"documentationUrl,omitempty"`
	ChangelogURL            string          `json:"changelogUrl,omitempty"`
	ConnectionSpecification json.RawMessage `json:"connectionSpecification"`
	SupportsIncremental     bool            `json:"supportsIncremental,omitempty"`

	// SupportedDestinationSyncModes is ignored by Flow
	SupportedDestinationSyncModes []DestinationSyncMode `json:"supported_destination_sync_modes,omitempty"`
	// SupportsNormalization is not currently used or supported by Flow or estuary-developed
	// connectors
	SupportsNormalization bool `json:"supportsNormalization,omitempty"`
	// SupportsDBT is not currently used or supported by Flow or estuary-developed
	// connectors
	SupportsDBT bool `json:"supportsDBT,omitempty"`
	// AuthSpecification is currently used by Flow for specifying the OAuth2Spec
	// part of SpecResponse in flow protocol, which allows for OAuth2 authorization
	// for connectors
	AuthSpecification json.RawMessage `json:"authSpecification,omitempty"`
	// AdvancedAuth is not currently used or supported by Flow or estuary-developed
	// connectors.
	AdvancedAuth json.RawMessage `json:"advanced_auth,omitempty"`
}

type MessageType string

const (
	MessageTypeRecord           MessageType = "RECORD"
	MessageTypeState            MessageType = "STATE"
	MessageTypeLog              MessageType = "LOG"
	MessageTypeSpec             MessageType = "SPEC"
	MessageTypeConnectionStatus MessageType = "CONNECTION_STATUS"
	MessageTypeCatalog          MessageType = "CATALOG"
)

type Message struct {
	Type             MessageType       `json:"type"`
	Log              *Log              `json:"log,omitempty"`
	State            *State            `json:"state,omitempty"`
	Record           *Record           `json:"record,omitempty"`
	ConnectionStatus *ConnectionStatus `json:"connectionStatus,omitempty"`
	Spec             *Spec             `json:"spec,omitempty"`
	Catalog          *Catalog          `json:"catalog,omitempty"`
}

func NewLogMessage(level LogLevel, msg string, args ...interface{}) Message {
	return Message{
		Type: MessageTypeLog,
		Log: &Log{
			Level:   level,
			Message: fmt.Sprintf(msg, args...),
		},
	}
}
