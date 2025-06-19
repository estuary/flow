// Package labels defines well-known label names and values of Gazette. Where
// sensible, recommended Kubernetes labels [1] are adopted by Gazette with
// identical meanings, but within the `app.gazette.dev` namespace.
//
// [1] https://kubernetes.io/docs/concepts/overview/working-with-objects/common-labels/
package labels

const (
	// ContentType of a journal, also known as a MIME or media-type. Must parse
	// as per RFC 1521. Only one ContentType label is allowed.
	ContentType = "content-type"

	// ContentType_CSV is the RFC 4180 mime type for CSV.
	ContentType_CSV = "text/csv"
	// ContentType_JSONLines is a ContentType for newline-delimited, JSON-encoded
	// messages. JSONLines is implemented by message.JSONFraming.
	ContentType_JSONLines = "application/x-ndjson"
	// ContentType_ProtoFixed is a ContentType for Protobuf messages delimited
	// by fixed header, consisting of a 4-byte "magic word" (for verifying stream
	// integrity) followed by a 4-byte little-endian message length, followed by
	// the packed Protobuf message. ProtoFixed is implemented by message.FixedFraming.
	ContentType_ProtoFixed = "application/x-protobuf-fixed"
	// ContentType_RecoveryLog is a ContentType for Gazette's recovery log encoding.
	// RecoveryLog is implemented by package `recoverylog`. To serve as a shard
	// recovery log, a JournalSpec must be labeled with ContentType_RecoveryLog.
	ContentType_RecoveryLog = "application/x-gazette-recoverylog"

	// MessageType of messages within the journal. Typically this will be a named
	// Protobuf message, struct name, or similar. Only one MessageType label
	// is allowed. If a MessageType label is present, a ContentType label must
	// also be present with a value drawn from FramedContentTypes.
	MessageType = "app.gazette.dev/message-type"
	// MessageSubType of messages of the journal. Applicable if the MessageType
	// represents a "union" composite type having multiple distinct sub-types.
	// MessageSubType names the specific subordinate type represented by messages
	// of the journal. Only one MessageSubType label is allowed, and if present,
	// than the MessageType label must be as well.
	MessageSubType = "app.gazette.dev/message-subtype"
	// Tag further refines or identifies journals and journal content. Multiple
	// user-defined Tag labels may be specified.
	Tag = "app.gazette.dev/tag"
	// ManagedBy names the tool or process which manages a specification.
	// All changes to the specification should be relegated to the named of ManagedBy.
	// Only one ManagedBy label is allowed. Compare to app.kubernetes.io/managed-by.
	ManagedBy = "app.gazette.dev/managed-by"
	// Instance identifies the specific application release to which a
	// specification pertains. Instance labels allow multiple releases of an
	// application to co-exist in the same cluster. See also:
	// of app.kubernetes.io/instance. Compare to app.kubernetes.io/instance.
	Instance = "app.gazette.dev/instance"
	// Region is the geographic region of the journal. Operators may wish to use
	// AWS, Azure, or GCP regions like "us-central1", "us-east-1", etc. Only one
	// Region label is allowed. Compare to failure-domain.beta.kubernetes.io/region.
	Region = "app.gazette.dev/region"
)

// SingleValueLabels identifies label names which must only have one label value
// within a specification.
var SingleValueLabels = map[string]struct{}{
	ContentType:    {},
	Instance:       {},
	ManagedBy:      {},
	MessageSubType: {},
	MessageType:    {},
	Region:         {},
}
