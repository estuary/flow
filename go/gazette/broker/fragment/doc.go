// Package fragment is concerned with the mapping of journal offsets to
// protocol.Fragments, to corresponding local or remote journal content. It
// provides implementation for:
//  * Interacting with remote fragment stores.
//  * Indexing local and remote Fragments (see Index).
//  * The construction of new Fragments from a replication stream (see Spool).
//  * The persisting of constructed Fragments to remote stores (see Persister).
package fragment
