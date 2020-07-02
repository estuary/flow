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
	// WorkerRing is the topology under which source collection documents
	// are shuffled to derivation workers, which can (slowly) change over time
	// as worker parallelism is increased or decreased. The value of this label
	// is an array of unsigned integers which defines a mapping from the Clock
	// time of a message UUID to the total number of workers.
	//
	// The rationale for synchronizing over message Clocks is that it allows for
	// atomic cut-overs by all workers from an old graph topology to an updated
	// one, so long as all workers are notified in advance: eg the cut-over
	// timestamp is sufficiently far in the future that the topology has propagated
	// to all processes. Workers use only the message Clock for this, never their
	// own measure of time (which could drift between workers).
	//
	// The label value is defined as:
	//   [num-workers, +delta-seconds, updated-num-workers, ...]
	//
	// For example, value [4, 1000, 8, 1000, 6] means:
	//  - Within interval [0, 1000), there are 4 workers.
	//  - Within interval [1000, 2000), there are 8 workers (scaled up).
	//  - Within interval [2000, ...), there are 6 workers (scaled down).
	WorkerRing = "estuary.dev/worker-ring"
	// Index of this Shard within the topology of workers for Derivation.
	WorkerIndex = "estuary.dev/worker-index"
)
