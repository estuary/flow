package labels

const (

	// UUID is a JournalSpec label whose value is a JSON-Pointer to a UUID
	// location within the message, for all messages of the journal.
	UUID = "estuary.dev/uuid"

	// Derive is a ShardSpec label whose value is a collection to be derived.
	Derive = "estuary.dev/derive"
)
