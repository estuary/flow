package materialize

import (
	"context"
	"encoding/json"
	"go.gazette.dev/core/consumer"
)

// TargetTransaction represents the remote store's view of the transaction.
type TargetTransaction interface {
	// Retrieves the current document from the remote store, or nil if it doesn't exist.
	FetchExistingDocument(primaryKey []interface{}) (json.RawMessage, error)
	// Stores the materialized document and all extractedFields.
	Store(extractedFields []interface{}, fullDocument json.RawMessage) error
}

// Target represents an external system that may hold a materialized view. This
// interface attempts to abstract over the details of any such system, whether it's a sql database
// or key-value store, or anything else.
type Target interface {
	consumer.Store
	// Starts a new transaction
	BeginTxn(_ context.Context) (TargetTransaction, error)
	// Returns a slice of all the location pointers to all projected fields. This slice must not
	// change over the lifetime of a running shard.
	ProjectionPointers() []string
	// Represents the primary keys of the collection as a slice of indexes into `ProjectionPointers`.
	// This also must not change over the lifetime of a running shard.
	PrimaryKeyFieldIndexes() []int
}
