// Package keyspace implements an efficient mechanism to mirror a decoded Etcd
// key/value space into a local KeySpace, which may be kept updated via a long-
// lived Watch operation. Each key & value of a KeySpace is decoded with a user
// provided decoder, and validated. Clients of a KeySpace are thus ensured that
// only validated keys & values are captured, while the KeySpace maintains
// consistency of the key/value set despite the potential for validation errors.
//
// KeySpace instances may be "observed", which allows additional states to be
// derived from and updated by the KeySpace while being protected by the
// KeySpace Mutex. For example, if a value `foo` is mutated by a function of
// KeySpace.Observers, then any reader which properly synchronizes over
// KeySpace.Mu is guaranteed to see values of `foo` which reflect the current
// KeySpace state. Formally, readers are assured atomicity of a combined
// update to the KeySpace and the derived value.
//
// KeySpace scales efficiently to Watches over 100's of thousands of keys by
// amortizing updates with a short Nagle-like delay, while providing fast range
// and point queries powered by its packed, sorted ordering.
package keyspace
