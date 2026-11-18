# discover

Merges a connector's `capture::response::Discovered` into a draft capture and
its target collections. Every discover shares it: the control plane's user and
auto-discovers (`control-plane-api::discovers`), and `flowctl raw discover`.

The merge is pure. Callers do the IO: running the connector, and fetching live
collections between the two phases.

## Entry points

- `parse`: resolves the form of a Discovered response into `Discovered`.
  An inline binding has a collection of its own. A `linked_collections` entry
  is one collection, shared by the bindings which reference it.
- `merge_bindings` (phase 1): merges discovered bindings into a `CaptureDef`,
  matching them to existing bindings by resource path. It returns a `Merged`
  carrying the `added` and `removed` bindings, and the `targets()` whose live
  specs phase 2 requires.
- `merge_collections` (phase 2): merges discovered keys and schemas into
  target collections, returning the bindings of `modified` collections.
- `Options::for_capture`: derives `update_only` and `reset_on_key_change`
  from the capture's `autoDiscover` stanza.

## Semantics

**Targets.** A matched binding keeps its target. An added binding targets its
normalized recommended name under the capture's prefix. Bindings sharing a
target "fan in" to it. A connector fans in bindings by giving them one
recommended name, and a user by re-targeting bindings. Either is permitted,
and bindings of a target needn't agree on their schemas or keys.

**Merging a target.** Each target is merged once, by `merge_collections`.
Its enabled bindings determine the merge, or all of its bindings if none are
enabled. So a disabled binding can't change a collection which an enabled
binding feeds.

- The last of these bindings, in binding order, provides the schema. Binding
  order is that of the connector's recommended names, as sent, and then
  of resource paths.
- The key changes only if each of these bindings discovered the same key.
  A rolling key migration across fanned-in tables changes the key once,
  after the last table migrates. The agreed key is a fallback key only if
  each binding flags it as one.

A connector may change one binding's recommended name. That binding keeps its
target, but moves in binding order. If it becomes the last enabled binding of
its target, its schema replaces the collection's. Its key can't, unless each
binding agrees.

**Drafting.** A collection already in the draft stays in it, modified or not.
Any other collection is drafted only if the merge modifies it. A new collection
behind a disabled binding is drafted too, so a user may enable the binding in
that draft. Publication prunes new collections which no enabled binding
references (`PruneUnboundCollections`).

A collection deleted in the draft is replaced by a new model of its discovered
collection, as if the deletion were published and then discovered. If the
collection is live, the replacement is reset. Bindings keep their `disable`.

**Reported changes.** `added` and `removed` are per binding. `modified`
reports each modified live collection once, under the first binding which
determined its merge. That binding may also be reported as added. The
`reason` of a modification describes each key change, and whether it reset
the collection. If the key waits on its bindings to agree, the `reason` of an
otherwise-modified collection says so.

**Name collisions.** Recommended names are normalized, but not case-folded.
Tables `Foo` and `foo` are distinct, and fanning them in to one collection is
unlikely to be what the user wants. They target distinct collections, which
publication rejects as colliding. The user resolves the collision explicitly.
