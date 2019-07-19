package keyspace

import (
	"bytes"
	"fmt"
	"sort"

	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/mvcc/mvccpb"
)

// KeyValue composes a "raw" Etcd KeyValue with its user-defined,
// decoded representation.
type KeyValue struct {
	Raw     mvccpb.KeyValue
	Decoded interface{}
}

// A KeyValueDecoder decodes raw KeyValue instances into a user-defined
// representation. KeyValueDecoder returns an error if the KeyValue cannot be
// decoded. KeySpace will log decoding errors and not incorporate them into
// the current KeyValues, but will also treat them as recoverable and in all
// cases seek to bring the KeyValues representation to consistency with Etcd.
// In practice, this means bad values written to Etcd which fail to decode
// may be later corrected with valid representations: KeySpace will ignore
// the bad update and then reflect the corrected one once available.
type KeyValueDecoder func(raw *mvccpb.KeyValue) (interface{}, error)

// KeyValues is a collection of KeyValue naturally ordered on keys.
type KeyValues []KeyValue

// Search returns the index at which |key| is found to be present,
// or should be inserted to maintain ordering.
func (kv KeyValues) Search(key string) (ind int, found bool) {
	ind = sort.Search(len(kv), func(i int) bool {
		return key <= string(kv[i].Raw.Key)
	})
	found = ind != len(kv) && key == string(kv[ind].Raw.Key)
	return
}

// Range returns the sub-slice of KeyValues spanning range [from, to).
func (kv KeyValues) Range(from, to string) KeyValues {
	var ind, _ = kv.Search(from)
	var tmp = kv[ind:]

	ind, _ = tmp.Search(to)
	return tmp[:ind]
}

// Prefixed returns the sub-slice of KeyValues prefixed by |prefix|.
func (kv KeyValues) Prefixed(prefix string) KeyValues {
	return kv.Range(prefix, clientv3.GetPrefixRangeEnd(prefix))
}

// Copy returns a deep-copy of the KeyValues.
func (kv KeyValues) Copy() KeyValues {
	var out = make(KeyValues, len(kv))

	for i, kv := range kv {
		out[i] = kv
	}
	return out
}

// EqualKeyRevisions returns true if keys and revisions of |other| matches
// this KeyValues.
func (kv KeyValues) EqualKeyRevisions(other KeyValues) bool {
	if len(other) != len(kv) {
		return false
	}
	for i := range kv {
		if kv[i].Raw.ModRevision != other[i].Raw.ModRevision {
			return false
		} else if !bytes.Equal(kv[i].Raw.Key, other[i].Raw.Key) {
			return false
		}
	}
	return true
}

// appendKeyValue attempts to decode and append the KeyValue to this KeyValues,
// or returns a decoding error. The appended KeyValue must order after all other
// keys, or appendKeyValue panics.
func appendKeyValue(kv KeyValues, decode KeyValueDecoder, cur *mvccpb.KeyValue) (KeyValues, error) {
	if len(kv) != 0 && bytes.Compare((kv)[len(kv)-1].Raw.Key, cur.Key) != -1 {
		panic("invalid key ordering")
	} else if decoded, err := decode(cur); err != nil {
		return kv, err
	} else {
		return append(kv, KeyValue{Raw: *cur, Decoded: decoded}), nil
	}
}

// updateKeyValuesTail updates the tail of KeyValues with the decoded event, and/or
// returns an error describing the event's inconsistency. |event| must reference
// a key which is the current tail of |kv|, or is ordered after the current tail,
// or updateKeyValuesTail panics. When returning errors indicating inconsistencies,
// in many cases events are still applied, in a best-effort attempt to keep KeyValues
// consistent in spite of the potential for KeyValueDecoder errors. In other
// words, human errors in crafting values which fail to decode must not break the
// overall consistency of a KeyValues instance, updated incrementally over time.
func updateKeyValuesTail(kv KeyValues, decode KeyValueDecoder, event clientv3.Event) (KeyValues, error) {
	var tail, cmp int

	if tail = len(kv) - 1; tail >= 0 {
		cmp = bytes.Compare(kv[tail].Raw.Key, event.Kv.Key)
	} else {
		cmp = -1 // Event key is ordered after empty KeyValues.
	}

	if cmp > 0 {
		panic("invalid key ordering (tail is ordered after event key)")
	} else if cmp < 0 {
		// Event key is ordered after |tail|.

		if event.Type == clientv3.EventTypeDelete {
			// This case can happen if a key with a bad value (which failed to decode,
			// and was not applied) is subsequently deleted. Ignoring the deletion
			// brings KeyValues back to consistency.
			return kv, fmt.Errorf("unexpected deletion of unknown key")
		} else if event.Type != clientv3.EventTypePut {
			// DELETE & PUT are the only defined types in Etcd's `mvccpb` package kv.proto.
			panic(event.Type)
		}

		if decoded, err := decode(event.Kv); err != nil {
			return kv, err
		} else {
			// Append a new value.
			kv = append(kv, KeyValue{Raw: *event.Kv, Decoded: decoded})
		}

		if event.Kv.CreateRevision != event.Kv.ModRevision {
			// Etcd creation events have matched Create & Mod revisions. We might then
			// expect this key to be present in the KeyValues. However, a creation with
			// a bad value (which is not applied) may be fixed by a future modification,
			// in which case the revisions may differ. Applying the update as if it were
			// a creation brings KeyValues back to consistency.
			return kv, fmt.Errorf("unexpected modification of unknown key (applied anyway)")
		} else if event.Kv.Version != 1 {
			// Etcd Versions should always be 1 at creation; this case should really never happen.
			return kv, fmt.Errorf("unexpected Version of created key (should be 1; applied anyway)")
		}
		return kv, nil
	}

	if kv[tail].Raw.ModRevision >= event.Kv.ModRevision {
		// This represents a replay of an old event, and should never happen (since
		// Events are observed with strictly monotonic Raft Revisions). Ignore the
		// update, as applying old data would make KeyValues inconsistent.
		return kv, fmt.Errorf(
			"unexpected ModRevision (it's too small; prev is %s)", kv[tail].Raw.String())
	}

	if event.Type == clientv3.EventTypeDelete {
		// Remove the current tail.
		return kv[:tail], nil
	} else if event.Type != clientv3.EventTypePut {
		panic(event.Type) // Only DELETE and PUT are defined.
	}

	var decoded, err = decode(event.Kv)
	if err != nil {
		return kv, err
	}

	if kv[tail].Raw.CreateRevision != event.Kv.CreateRevision {
		// This case is possible only if an interleaved deletion of this key was
		// somehow missed (not sent by the Etcd server), and should never happen.
		// If it does, apply the event anyway as it still reflects the latest value
		// for this key.
		err = fmt.Errorf(
			"unexpected CreateRevision (should be equal; applied anyway; prev is %s)", kv[tail].Raw.String())
	} else if kv[tail].Raw.Version+1 != event.Kv.Version {
		// This case is possible if an interleaved modification of this key was
		// somehow missed (not sent by the Etcd server), OR (and far more likely),
		// an existing key was modified with a bad value, which was not applied,
		// and we're now patching a subsequent correction. In this case, Version
		// will appear to skip a value.
		err = fmt.Errorf(
			"unexpected Version (should be monotonic; applied anyway; prev is %s)", kv[tail].Raw.String())
	}

	// Update existing value at |ind|.
	kv[tail] = KeyValue{Raw: *event.Kv, Decoded: decoded}
	return kv, err
}
