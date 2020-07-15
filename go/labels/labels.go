package labels

// JournalSpec labels.
const (
	// Collection is the name of the Estuary collection for which this Journal
	// holds documents.
	Collection = "estuary.dev/collection"
	// Field is a logical partition of the Collection that's implemented by this
	// journal.
	FieldPrefix = "estuary.dev/field/"
	// UUIDPointer is a JSON-Pointer which resolves to the location of UUIDs
	// within documents of this journal.
	UUIDPointer = "estuary.dev/uuid-ptr"
	// ACKTemplate is a valid document which models transaction acknowledgements
	// of this journal, and contains a placeholder UUID.
	ACKTemplate = "estuary.dev/ack-template"
)

// ShardSpec labels.
const (
	// CatalogURL is the URL of the catalog that's processed by this Shard.
	CatalogURL = "estuary.dev/catalog-url"
	// Derivation is the name of the Estuary collection to be derived.
	Derivation = "estuary.dev/derivation"
	// Index of this Shard within the topology of workers for Derivation.
	WorkerIndex = "estuary.dev/worker-index"
)
