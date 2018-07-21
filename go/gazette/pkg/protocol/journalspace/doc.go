// Package journalspace provides mechanisms for mapping a collection of
// JournalSpecs into a minimally-described hierarchical structure, and for
// mapping back again. This is principally useful for tooling over JournalSpecs,
// which must be written to (& read from) Etcd in fully-specified and explicit
// form (a representation which is great for implementors, but rather tedious
// for cluster operators to work with). Tooling can map JournalSpecs into a
// tree, allow the operator to apply edits in that hierarchical space, and then
// flatten resulting changes for storage back to Etcd.
package journalspace
