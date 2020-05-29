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
	committedBytesTotal = promauto.NewCounter(prometheus.CounterOpts{
		Name: "gazette_committed_bytes_total",
		Help: "Cumulative number of bytes committed to journals fragment spools (across all replicas)",
	})
	commitsTotal = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "gazette_commits_total",
		Help: "DEPRECATED Cumulative number of commits to journal spools",
	}, []string{"status"})
)
