package labels

// JournalSpec labels.
const (
	// Collection is the name of the Estuary collection for which this Journal
	// holds documents.
	Collection = "estuary.dev/collection"
	// Field is a logical partition of the Collection that's implemented by this
	// journal.
	FieldPrefix = "estuary.dev/field/"
)

// ShardSpec labels.
const (
	// CatalogURL is the URL of the catalog that's processed by this Shard.
	// The CatalogURL of a ShardSpec may change over time.
	// A running consumer detects and applies changes to the CatalogURL.
	CatalogURL = "estuary.dev/catalog-url"
	// Derivation is the name of the Estuary collection to be derived.
	// Once set on a ShardSpec, it cannot change.
	Derivation = "estuary.dev/derivation"
	// Index of this Shard within the topology of workers for Derivation.
	// Once set on a ShardSpec, it cannot change.
	WorkerIndex = "estuary.dev/worker-index"
)
