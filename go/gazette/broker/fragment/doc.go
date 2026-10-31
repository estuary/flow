// Package fragment is a broker-only package concerned with the mapping of journal offsets to
// protocol.Fragments, and from there to corresponding local or remote journal content.
//
// It implements file-like operations over the FragmentStore schemes supported
// by Gazette, such as listing, opening, signing, persisting, and removing fragments.
// See FileStoreConfig, S3StoreConfig, and GSStoreConfig for further configuration
// of store operations.
//
// The package implements a Fragment wrapper type which composes a protocol.Fragment
// with an open file descriptor, and an Index over local or remote Fragments which maps
// a journal offset to a best-covering Fragment.
//
// Spool is a Fragment which is in the process of being constructed from an ongoing
// broker Replicate RPC. It is the transactional "memory" of brokers which are
// participating in the replication of a journal. Once closed, or "rolled", a Spool
// Fragment is persisted to its configured FragmentStore by a Persister.
package fragment

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	spoolCommitsTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gazette_spool_commits_total",
		Help: "Total number of commits of journal fragment spools.",
	})
	spoolCommitBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gazette_spool_commit_bytes_total",
		Help: "Total number of bytes committed to journal fragment spools.",
	})
	spoolRollbacksTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gazette_spool_rollbacks_total",
		Help: "Total number of rollbacks of journal fragment spools.",
	})
	spoolRollbackBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gazette_spool_rollback_bytes_total",
		Help: "Total number of bytes rolled-back from journal fragment spools.",
	})
	spoolCompletedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gazette_spool_completed_total",
		Help: "Total number of journal fragment spools which have been completed, and are ready for persisting.",
	})
	spoolPersistedTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gazette_spool_persisted_total",
		Help: "Total number of journal fragment spools which were persisted (by this server, or by another and then verified by this server).",
	})
)
