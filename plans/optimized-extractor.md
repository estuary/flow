# ExtractorPlan: Merge-Join Optimization for Field Extraction

## Motivation

Materializations project each output field by invoking
`doc::Extractor::extract_all_indicate_truncation`, which calls
`Pointer::query` once per extractor. For a binding with N projected fields on
a common parent object, that's N independent walks from the document root
down through the shared parent, plus N lookups against the parent's fields.

Real bindings commonly have many sibling-leaf fields under one parent
(`/after/col_*`, `/before/col_*`, etc.), so this is N walks of nearly
identical work followed by N `Fields::get` calls on the same parent. Each
`Fields::get` is `O(log F)` against the parent's sorted field list.

If the extractor list is sorted by field name, that sequence collapses to a
single merge-join: walk to the parent once, then step through the parent's
fields and the extractor list in tandem in `O(F + N)`. The win compounds
with parent depth and section width.

## Work to do

1. Add `crates/doc/src/extractor_plan.rs` defining `ExtractorPlan`, a
   pre-compiled plan over a `Vec<Extractor>`.
2. Refactor `crates/doc/src/extractor.rs` to expose the building blocks
   the plan needs (see dedicated section below).
3. Wire the plan into the materialize runtime: replace
   `Binding.value_extractors` with `Binding.value_plan: doc::ExtractorPlan`.
4. Add tests: a differential fuzz test (`extractor_plan_fuzz.rs`) and a
   performance benchmark harness (`extractor_perf.rs`).

## `ExtractorPlan`: design

`ExtractorPlan::new` should scan the extractor list for **blocks**:
maximal runs of two or more consecutive extractors that are all
sibling-leaves (`parent_ptr + /property`) under a single common parent
with field names in monotonically ascending order. Singletons don't
form a block — they'd pay the block's setup cost (parent walk, object
check, iterator construction) with no sharing benefit, and degrade the
field lookup from `Fields::get` (binary search) to a linear merge
advance.

At extract time:
- Extractors outside any block should flow through the reference path
  (`write_extracted`), which does a per-extractor `Pointer::query`.
- For each block, walk once to the parent pointer (`emit_block`). If
  the parent resolves to an object, do a two-pointer walk of the
  parent's fields and the block's field names
  (`merge_write_block_extractors`). If the parent is missing, null, or
  a non-object, every block extractor receives `None` and uses its
  default.

The plan must preserve the exact output bytes of the reference path,
including truncation-indicator bookkeeping and the ordering of defaults
when fields are missing. Enforce that invariant with the differential
fuzz test.

### Ineligibility rules (keep deliberately simple)

- Root-document extractor (`Pointer` empty) — no parent to merge against.
- Array-index terminal (`/arr/0`) — merge-join is over object field
  names, not positional indices.
- Truncation indicator — must write its placeholder at its own tuple
  position, so it breaks a run even if it sits between otherwise-eligible
  siblings.
- Non-ascending field names — the merge-join relies on monotonic
  ordering; a back-step splits the run.
- Different parents — different parent pointers never fuse.

Note on sort violations: splitting a run when field names aren't in
ascending order is intentionally all-or-nothing. We deliberately skip
the obvious next step — reorder the block into sorted form, merge-join,
then scatter values back into their original output positions — because
field selection for materializations almost always emits fields in
field-name order already. Treating reordering as a non-goal keeps both
the planner and the runtime straightforward.

## Refactor of `extractor.rs` to support the plan

`ExtractorPlan` needs almost all of `Extractor`'s internals — policy
application, UUID magic, truncation-indicator accounting, default
substitution, tuple packing. Duplicating any of that would be fragile,
so the job here is to lift the right seams out of `Extractor` and make
them reachable from the plan module without changing observable
behavior.

The refactor should be structural only. Every reshuffle needs a
corresponding inlined call at the old site so the pre-refactor
`extract_all_indicate_truncation` and `extract_indicate_truncation`
still work byte-for-byte the same.

### New `PlanKind` classifier (crate-visible)

Add a small enum (`PlanKind<'a>`) returned by a new
`Extractor::plan_kind()` method, which inspects `self.ptr`'s terminal
token and `self.magic`. The plan-compile scan uses this to decide
eligibility without reaching into `Extractor`'s private fields. Three
variants:

- `MergeJoinLeaf { parent, name }` — an extractor whose pointer ends in
  an object-property token. `parent` is the shared prefix slice;
  `name` is the property name. These are the candidates for blocks.
- `TruncationIndicator` — must run in place through the reference path.
- `Other` — root or array-index terminal; not block-eligible.

### Split `query` into resolved/unresolved halves

`Extractor::query` currently does two things: resolve `self.ptr`
against the doc, then apply magic and defaults to the resolved node.
Split those steps:

- `fn query(...)` — keep the signature; have it delegate to
  `value_from_resolved(self.ptr.query(doc))`.
- `fn value_from_resolved(...)` — new; takes an `Option<&N>` that has
  already been resolved by some other means (in the plan's case, by
  the merge-join). Contains the magic / default logic previously
  embedded in `query`.

This split is what lets the plan skip the per-extractor
`Pointer::query` while still routing the resolved node through the
exact same magic / default / truncation pipeline.

### New `extract_from_resolved_indicate_truncation`

Add a sibling of `extract_indicate_truncation` that takes a
pre-resolved `Option<&N>` instead of walking `self.ptr` itself. This
is the method `merge_write_block_extractors` and `emit_block` will
call on a per-extractor basis after the merge-join has already
resolved (or failed to resolve) the leaf. Make it crate-visible only.

Have the existing `extract_indicate_truncation` trivially delegate to
it.

### Lift shared helpers out of `extract_all_indicate_truncation`

`Extractor::extract_all_indicate_truncation` currently inlines two
pieces of logic the plan also needs:

- The per-slice write loop, including the debug-assert that catches
  multiple truncation-indicator projections and the
  `projected_indicator_pos` bookkeeping.
- The retroactive flip of the indicator placeholder byte
  (`0x26` → `0x27`) once the full extract is done.

Lift those into two crate-visible free functions:

- `write_extracted(doc, extractors, out, indicator, &mut projected_indicator_pos)`
- `finalize_truncation_indicator(out, projected_indicator_pos, indicator)`

Have `Extractor::extract_all_indicate_truncation` thread a local
`projected_indicator_pos` through both, and have
`ExtractorPlan::extract_all_indicate_truncation` do the same — calling
`write_extracted` for the gap ranges between blocks and once more at
the tail, with a single `finalize_truncation_indicator` at the end.
Keeping the bookkeeping in one place is what makes it safe for the
plan to interleave merge-join blocks with reference-path gaps without
losing the single-indicator invariant.

### Small helper: `is_truncation_indicator`

Add a crate-visible `Extractor::is_truncation_indicator()` that
replaces the direct `self.magic == Some(Magic::TruncationIndicator)`
check inside the old `extract_all_indicate_truncation`. Both
`write_extracted` and `plan_kind` will use it, and it avoids leaking
the private `Magic` enum to the plan module.

## Materialize runtime integration

In `crates/runtime/src/materialize/`:

- `mod.rs` — replace `Binding.value_extractors: Vec<doc::Extractor>`
  with `Binding.value_plan: doc::ExtractorPlan`. Build the plan once
  per binding at task-load time and reuse it for every document.
- `task.rs` — in `Binding::build`, call `doc::ExtractorPlan::new(...)`
  on the result of `extractors::for_fields(selected_values, ...)`.
  Leave key extractors unchanged (keys are a small, fixed set where
  merge-join wouldn't pay off).
- `protocol.rs` — have `send_connector_store` call
  `binding.value_plan.extract_all_owned_indicate_truncation(...)`
  instead of `doc::Extractor::extract_all_owned_indicate_truncation`.
- `triggers.rs` — update the test fixture to construct
  `doc::ExtractorPlan::new(&[])`.

Wire up only value extraction in this change. Key extraction should
stay on the reference path.

## Tests

### `crates/doc/tests/extractor_plan_fuzz.rs` — differential fuzz

QuickCheck-driven. For each generated `PlanSpec`, build both an
`Extractor`-only reference pack and an `ExtractorPlan` pack over the
same doc and assert byte equality. Run over `serde_json::Value`,
`HeapNode`, and `ArchivedNode` so all three `AsNode` implementations
are covered.

Construct the generator to stress the planner, not to be uniformly
random: every `PlanSpec` should contain three blocks (`/wide`,
`/nested/inner`, and an intentionally-unusable parent like
`/missing_parent` or `/arr`), interleaved with random singles,
truncation indicator, UUID magic, and three policy variants (noop,
string truncation, aggressive truncation).

### `crates/doc/tests/extractor_perf.rs` — benchmark harness

Not a correctness test; a performance harness that reports ns/doc for
`Extractor` vs `ExtractorPlan` over six representative shapes:

- `single wide block` — happy path: one 64-field top-level block.
- `nested blocks with singles` — two nested 32-field blocks + metadata /
  UUID / truncation-indicator singles. Closest to production shape.
- `no blocks` — many singles under parents that never reach the
  merge-join threshold. Verifies the plan doesn't regress here.
- `sparse block, large parent` — 32 extractors over a 512-field parent.
  The risk case: the merge scan visits many unprojected fields.
- `many small blocks` — 8 parents of 10 fields each. Per-block fixed
  overhead case.
- `deeply nested blocks` — 5 parents at increasing depth, 100 fields
  each. Demonstrates how parent-walk reuse compounds with depth.

Run `TOTAL_ROUNDS = 100` per case against both `HeapNode` and
`ArchivedNode`. Include it in CI so it keeps compiling, but note that
meaningful numbers require `cargo test --release -p doc --test
extractor_perf -- --nocapture`.
