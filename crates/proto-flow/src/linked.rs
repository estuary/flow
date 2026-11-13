//! Accessors which resolve a binding or transform to its CollectionSpec,
//! transparently over both the inlined and indirect encodings.
//!
//! A spec-carrying message is in *indirect* form when its `linked_collections`
//! table is non-empty: every binding then leaves `collection` unset and names
//! its collection through `collection_index`. When the table is empty the
//! message is in *inline* form and every binding carries its own
//! `collection`, exactly as it always has. The form is a property of the
//! message as a whole, so these accessors branch on the table and never on a
//! per-binding presence check.
//!
//! Readers never normalize: a spec keeps whichever form it arrived in, and
//! readers go through these accessors instead of caring.
//!
//! Writers convert between forms with the `indirect` and `inline` methods of
//! each message. Collections are shared (`crate::Arc`), so converting copies
//! pointers and never a collection.

use crate::{capture, derive, flow, materialize};

/// A resolved collection paired with its identity within the parent message.
///
/// The identity is `Some(index)` into the parent's `linked_collections` when
/// the parent is in indirect form, and `None` when the binding inlines its own
/// collection -- in which case the binding *is* the identity. Callers which
/// memoize per-collection derived state (validators, extractors, projections)
/// key on this, falling back to per-binding state when it's `None`.
pub type Resolved<'a> = (&'a crate::Arc<flow::CollectionSpec>, Option<u32>);

/// Resolve one binding's collection against its parent's table.
///
/// Returns None if an indirect-form `collection_index` is out of bounds, or if an
/// inline-form binding is missing its collection. Both are malformed
/// messages, and returning None lets call sites attach their own context
/// rather than panicking here.
fn resolve<'a>(
    linked_collections: &'a [crate::Arc<flow::CollectionSpec>],
    inline: Option<&'a crate::Arc<flow::CollectionSpec>>,
    collection_index: u32,
) -> Option<Resolved<'a>> {
    if linked_collections.is_empty() {
        inline.map(|collection| (collection, None))
    } else {
        linked_collections
            .get(collection_index as usize)
            .map(|collection| (collection, Some(collection_index)))
    }
}

impl flow::CaptureSpec {
    /// Resolve `binding`'s collection. See [`Resolved`].
    pub fn binding_collection<'a>(
        &'a self,
        binding: &'a flow::capture_spec::Binding,
    ) -> Option<Resolved<'a>> {
        resolve(
            &self.linked_collections,
            binding.collection.as_ref(),
            binding.collection_index,
        )
    }

    /// Iterate active bindings, each paired with its resolved collection.
    pub fn resolved_bindings(
        &self,
    ) -> impl Iterator<Item = (&flow::capture_spec::Binding, Option<Resolved<'_>>)> {
        self.bindings
            .iter()
            .map(|binding| (binding, self.binding_collection(binding)))
    }

    /// Iterate inactive bindings, each paired with its resolved collection.
    /// Inactive bindings share the table of their active peers.
    pub fn resolved_inactive_bindings(
        &self,
    ) -> impl Iterator<Item = (&flow::capture_spec::Binding, Option<Resolved<'_>>)> {
        self.inactive_bindings
            .iter()
            .map(|binding| (binding, self.binding_collection(binding)))
    }

    /// Iterate active bindings followed by inactive ones.
    pub fn resolved_all_bindings(
        &self,
    ) -> impl Iterator<Item = (&flow::capture_spec::Binding, Option<Resolved<'_>>)> {
        self.resolved_bindings()
            .chain(self.resolved_inactive_bindings())
    }
}

impl flow::MaterializationSpec {
    /// Resolve `binding`'s collection. See [`Resolved`].
    pub fn binding_collection<'a>(
        &'a self,
        binding: &'a flow::materialization_spec::Binding,
    ) -> Option<Resolved<'a>> {
        resolve(
            &self.linked_collections,
            binding.collection.as_ref(),
            binding.collection_index,
        )
    }

    /// Iterate active bindings, each paired with its resolved collection.
    pub fn resolved_bindings(
        &self,
    ) -> impl Iterator<Item = (&flow::materialization_spec::Binding, Option<Resolved<'_>>)> {
        self.bindings
            .iter()
            .map(|binding| (binding, self.binding_collection(binding)))
    }

    /// Iterate inactive bindings, each paired with its resolved collection.
    /// Inactive bindings share the table of their active peers.
    pub fn resolved_inactive_bindings(
        &self,
    ) -> impl Iterator<Item = (&flow::materialization_spec::Binding, Option<Resolved<'_>>)> {
        self.inactive_bindings
            .iter()
            .map(|binding| (binding, self.binding_collection(binding)))
    }

    /// Iterate active bindings followed by inactive ones.
    pub fn resolved_all_bindings(
        &self,
    ) -> impl Iterator<Item = (&flow::materialization_spec::Binding, Option<Resolved<'_>>)> {
        self.resolved_bindings()
            .chain(self.resolved_inactive_bindings())
    }
}

impl flow::collection_spec::Derivation {
    /// Resolve `transform`'s source collection. See [`Resolved`].
    ///
    /// This resolves only the *sources* of transforms. The derived collection
    /// which owns this Derivation is never indirected.
    pub fn transform_collection<'a>(
        &'a self,
        transform: &'a flow::collection_spec::derivation::Transform,
    ) -> Option<Resolved<'a>> {
        resolve(
            &self.linked_collections,
            transform.collection.as_ref(),
            transform.collection_index,
        )
    }

    /// Iterate active transforms, each paired with its resolved source collection.
    pub fn resolved_transforms(
        &self,
    ) -> impl Iterator<
        Item = (
            &flow::collection_spec::derivation::Transform,
            Option<Resolved<'_>>,
        ),
    > {
        self.transforms
            .iter()
            .map(|transform| (transform, self.transform_collection(transform)))
    }

    /// Iterate inactive transforms, each paired with its resolved source
    /// collection. Inactive transforms share the table of their active peers.
    pub fn resolved_inactive_transforms(
        &self,
    ) -> impl Iterator<
        Item = (
            &flow::collection_spec::derivation::Transform,
            Option<Resolved<'_>>,
        ),
    > {
        self.inactive_transforms
            .iter()
            .map(|transform| (transform, self.transform_collection(transform)))
    }

    /// Iterate active transforms followed by inactive ones.
    pub fn resolved_all_transforms(
        &self,
    ) -> impl Iterator<
        Item = (
            &flow::collection_spec::derivation::Transform,
            Option<Resolved<'_>>,
        ),
    > {
        self.resolved_transforms()
            .chain(self.resolved_inactive_transforms())
    }
}

impl capture::request::Validate {
    /// Resolve `binding`'s collection. See [`Resolved`].
    pub fn binding_collection<'a>(
        &'a self,
        binding: &'a capture::request::validate::Binding,
    ) -> Option<Resolved<'a>> {
        resolve(
            &self.linked_collections,
            binding.collection.as_ref(),
            binding.collection_index,
        )
    }

    /// Iterate bindings, each paired with its resolved collection.
    pub fn resolved_bindings(
        &self,
    ) -> impl Iterator<Item = (&capture::request::validate::Binding, Option<Resolved<'_>>)> {
        self.bindings
            .iter()
            .map(|binding| (binding, self.binding_collection(binding)))
    }
}

impl materialize::request::Validate {
    /// Resolve `binding`'s collection. See [`Resolved`].
    pub fn binding_collection<'a>(
        &'a self,
        binding: &'a materialize::request::validate::Binding,
    ) -> Option<Resolved<'a>> {
        resolve(
            &self.linked_collections,
            binding.collection.as_ref(),
            binding.collection_index,
        )
    }

    /// Iterate bindings, each paired with its resolved collection.
    pub fn resolved_bindings(
        &self,
    ) -> impl Iterator<
        Item = (
            &materialize::request::validate::Binding,
            Option<Resolved<'_>>,
        ),
    > {
        self.bindings
            .iter()
            .map(|binding| (binding, self.binding_collection(binding)))
    }
}

impl derive::request::Validate {
    /// Resolve `transform`'s source collection. See [`Resolved`].
    ///
    /// This resolves only the *sources* of transforms. The request's derived
    /// `collection` and its `last_collection` are never indirected.
    pub fn transform_collection<'a>(
        &'a self,
        transform: &'a derive::request::validate::Transform,
    ) -> Option<Resolved<'a>> {
        resolve(
            &self.linked_collections,
            transform.collection.as_ref(),
            transform.collection_index,
        )
    }

    /// Iterate transforms, each paired with its resolved source collection.
    pub fn resolved_transforms(
        &self,
    ) -> impl Iterator<Item = (&derive::request::validate::Transform, Option<Resolved<'_>>)> {
        self.transforms
            .iter()
            .map(|transform| (transform, self.transform_collection(transform)))
    }
}

/// Interns collections into a `linked_collections` table, collapsing
/// bindings which share a collection.
///
/// Entries are unique by *value*, not by name: two bindings of one collection
/// whose `group_by` differs carry differing `key` and `projections`, and are
/// distinct entries which happen to share a name.
#[derive(Default)]
pub struct Interner {
    specs: Vec<crate::Arc<flow::CollectionSpec>>,
    // Candidate indices into `specs` for each collection name. Names are
    // rarely duplicated, so a linear scan over candidates is cheap, and
    // candidates which share `spec`'s allocation are equal without a walk.
    by_name: std::collections::HashMap<String, Vec<u32>>,
}

impl Interner {
    /// Intern `spec`, returning its issued index.
    pub fn intern(&mut self, spec: crate::Arc<flow::CollectionSpec>) -> u32 {
        let candidates = self.by_name.entry(spec.name.clone()).or_default();

        if let Some(index) = candidates
            .iter()
            .find(|index| self.specs[**index as usize] == spec)
        {
            return *index;
        }
        let index = self.specs.len() as u32;
        candidates.push(index);
        self.specs.push(spec);
        index
    }

    /// Yield the finished table, ordered by collection name, alongside a remap
    /// from each index issued by `intern` into the ordered table.
    ///
    /// Interning happens in binding order while the table is conventionally
    /// ordered by name, and the remap reconciles the two. The ordering is
    /// stable, so entries sharing a name keep their interned order.
    fn finish(self) -> (Vec<crate::Arc<flow::CollectionSpec>>, Vec<u32>) {
        let Interner { specs, by_name: _ } = self;

        let mut order: Vec<u32> = (0..specs.len() as u32).collect();
        order.sort_by(|l, r| specs[*l as usize].name.cmp(&specs[*r as usize].name));

        let mut remap = vec![0; specs.len()];
        for (ordered, issued) in order.iter().enumerate() {
            remap[*issued as usize] = ordered as u32;
        }
        let table = order
            .iter()
            .map(|issued| specs[*issued as usize].clone())
            .collect();

        (table, remap)
    }
}

// A `collection_index` which never resolves and which `Interner` never issues.
// `indirect` assigns it to a binding that has no collection, so that the
// binding remains unresolvable.
const UNRESOLVED: u32 = u32::MAX;

/// Define the form conversions of `$msg` over its `$bindings` lists.
macro_rules! convert {
    ($msg:ty, $($bindings:ident),+) => {
        impl $msg {
            /// Install `interner` as the `linked_collections` of this
            /// message, which is then in indirect form. Each binding must
            /// carry a `collection_index` issued by `interner`, and no
            /// `collection`.
            pub fn install(&mut self, interner: Interner) {
                let (table, remap) = interner.finish();

                $(for binding in self.$bindings.iter_mut() {
                    if binding.collection_index != UNRESOLVED {
                        binding.collection_index = remap[binding.collection_index as usize];
                    }
                })+
                self.linked_collections = table;
            }

            /// Convert this message to indirect form, if it's not already.
            /// A binding which has no collection continues to not resolve.
            pub fn indirect(&mut self) {
                if !self.linked_collections.is_empty() {
                    return;
                }
                let mut interner = Interner::default();

                $(for binding in self.$bindings.iter_mut() {
                    binding.collection_index = match binding.collection.take() {
                        Some(collection) => interner.intern(collection),
                        None => UNRESOLVED,
                    };
                })+
                self.install(interner);
            }

            /// Convert this message to inline form, if it's not already.
            /// A binding whose `collection_index` doesn't resolve is left
            /// without a collection.
            pub fn inline(&mut self) {
                if self.linked_collections.is_empty() {
                    return;
                }
                let table = std::mem::take(&mut self.linked_collections);

                $(for binding in self.$bindings.iter_mut() {
                    binding.collection = table.get(binding.collection_index as usize).cloned();
                    binding.collection_index = 0;
                })+
            }
        }
    };
}

convert!(flow::CaptureSpec, bindings, inactive_bindings);
convert!(flow::MaterializationSpec, bindings, inactive_bindings);
convert!(
    flow::collection_spec::Derivation,
    transforms,
    inactive_transforms
);
convert!(capture::request::Validate, bindings);
convert!(materialize::request::Validate, bindings);
convert!(derive::request::Validate, transforms);

#[cfg(test)]
mod test {
    use super::*;

    fn collection(name: &str) -> crate::Arc<flow::CollectionSpec> {
        crate::Arc::new(flow::CollectionSpec {
            name: name.to_string(),
            ..Default::default()
        })
    }

    // Names of the collections resolved by an iterator, where an unresolvable
    // binding renders as "!".
    fn names<'a, B: 'a>(
        it: impl Iterator<Item = (&'a B, Option<Resolved<'a>>)>,
    ) -> Vec<(String, Option<u32>)> {
        it.map(|(_binding, resolved)| match resolved {
            Some((collection, index)) => (collection.name.clone(), index),
            None => ("!".to_string(), None),
        })
        .collect()
    }

    #[test]
    fn capture_spec_inline_form() {
        let spec = flow::CaptureSpec {
            bindings: vec![
                flow::capture_spec::Binding {
                    collection: Some(collection("acmeCo/one")),
                    ..Default::default()
                },
                // A stray `collection_index` is ignored in inline form.
                flow::capture_spec::Binding {
                    collection: Some(collection("acmeCo/two")),
                    collection_index: 7,
                    ..Default::default()
                },
                // A binding missing its collection doesn't resolve.
                flow::capture_spec::Binding::default(),
            ],
            inactive_bindings: vec![flow::capture_spec::Binding {
                collection: Some(collection("acmeCo/gone")),
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(
            names(spec.resolved_all_bindings()),
            [
                ("acmeCo/one".to_string(), None),
                ("acmeCo/two".to_string(), None),
                ("!".to_string(), None),
                ("acmeCo/gone".to_string(), None),
            ],
        );
    }

    #[test]
    fn capture_spec_indirect_form() {
        let spec = flow::CaptureSpec {
            linked_collections: vec![collection("acmeCo/one"), collection("acmeCo/two")],
            bindings: vec![
                flow::capture_spec::Binding {
                    collection_index: 1,
                    ..Default::default()
                },
                // Many bindings may share one collection.
                flow::capture_spec::Binding {
                    collection_index: 1,
                    ..Default::default()
                },
                flow::capture_spec::Binding::default(), // Index zero.
                // Out-of-bounds doesn't resolve.
                flow::capture_spec::Binding {
                    collection_index: 2,
                    ..Default::default()
                },
                // An inlined collection is shadowed by the table: the form is
                // a property of the message, not of the binding.
                flow::capture_spec::Binding {
                    collection: Some(collection("acmeCo/ignored")),
                    collection_index: 0,
                    ..Default::default()
                },
            ],
            // Inactive bindings index the same table.
            inactive_bindings: vec![flow::capture_spec::Binding {
                collection_index: 1,
                ..Default::default()
            }],
            ..Default::default()
        };

        assert_eq!(
            names(spec.resolved_all_bindings()),
            [
                ("acmeCo/two".to_string(), Some(1)),
                ("acmeCo/two".to_string(), Some(1)),
                ("acmeCo/one".to_string(), Some(0)),
                ("!".to_string(), None),
                ("acmeCo/one".to_string(), Some(0)),
                ("acmeCo/two".to_string(), Some(1)),
            ],
        );
        assert_eq!(names(spec.resolved_bindings()).len(), 5);
        assert_eq!(names(spec.resolved_inactive_bindings()).len(), 1);
    }

    #[test]
    fn materialization_spec_both_forms() {
        let inline = flow::MaterializationSpec {
            bindings: vec![flow::materialization_spec::Binding {
                collection: Some(collection("acmeCo/one")),
                ..Default::default()
            }],
            inactive_bindings: vec![flow::materialization_spec::Binding::default()],
            ..Default::default()
        };
        assert_eq!(
            names(inline.resolved_all_bindings()),
            [("acmeCo/one".to_string(), None), ("!".to_string(), None),],
        );

        // Duplicate names with distinct values are legal, and are what a
        // differing `group_by` between two bindings of one collection produces.
        let mut keyed = collection("acmeCo/one");
        crate::Arc::make_mut(&mut keyed).key = vec!["/id".to_string()];

        let indirect = flow::MaterializationSpec {
            linked_collections: vec![collection("acmeCo/one"), keyed.clone()],
            bindings: vec![
                flow::materialization_spec::Binding::default(),
                flow::materialization_spec::Binding {
                    collection_index: 1,
                    ..Default::default()
                },
                flow::materialization_spec::Binding {
                    collection_index: 99,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert_eq!(
            names(indirect.resolved_bindings()),
            [
                ("acmeCo/one".to_string(), Some(0)),
                ("acmeCo/one".to_string(), Some(1)),
                ("!".to_string(), None),
            ],
        );
        // The two same-name entries are distinguished by their identity.
        assert_eq!(indirect.linked_collections[1].key, keyed.key);
    }

    #[test]
    fn derivation_both_forms() {
        let inline = flow::collection_spec::Derivation {
            transforms: vec![flow::collection_spec::derivation::Transform {
                collection: Some(collection("acmeCo/src")),
                ..Default::default()
            }],
            inactive_transforms: vec![flow::collection_spec::derivation::Transform::default()],
            ..Default::default()
        };
        assert_eq!(
            names(inline.resolved_all_transforms()),
            [("acmeCo/src".to_string(), None), ("!".to_string(), None)],
        );

        let indirect = flow::collection_spec::Derivation {
            linked_collections: vec![collection("acmeCo/src")],
            transforms: vec![
                flow::collection_spec::derivation::Transform::default(),
                flow::collection_spec::derivation::Transform {
                    collection_index: 1,
                    ..Default::default()
                },
            ],
            inactive_transforms: vec![flow::collection_spec::derivation::Transform::default()],
            ..Default::default()
        };
        assert_eq!(
            names(indirect.resolved_all_transforms()),
            [
                ("acmeCo/src".to_string(), Some(0)),
                ("!".to_string(), None),
                ("acmeCo/src".to_string(), Some(0)),
            ],
        );
    }

    #[test]
    fn validate_requests_both_forms() {
        let inline = capture::request::Validate {
            bindings: vec![capture::request::validate::Binding {
                collection: Some(collection("acmeCo/one")),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            names(inline.resolved_bindings()),
            [("acmeCo/one".to_string(), None)],
        );

        let indirect = capture::request::Validate {
            linked_collections: vec![collection("acmeCo/one")],
            bindings: vec![
                capture::request::validate::Binding::default(),
                capture::request::validate::Binding {
                    collection_index: 5,
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        assert_eq!(
            names(indirect.resolved_bindings()),
            [("acmeCo/one".to_string(), Some(0)), ("!".to_string(), None)],
        );

        let indirect = materialize::request::Validate {
            linked_collections: vec![collection("acmeCo/one"), collection("acmeCo/two")],
            bindings: vec![materialize::request::validate::Binding {
                collection_index: 1,
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            names(indirect.resolved_bindings()),
            [("acmeCo/two".to_string(), Some(1))],
        );

        let inline = materialize::request::Validate {
            bindings: vec![materialize::request::validate::Binding {
                collection: Some(collection("acmeCo/two")),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            names(inline.resolved_bindings()),
            [("acmeCo/two".to_string(), None)],
        );

        let indirect = derive::request::Validate {
            linked_collections: vec![collection("acmeCo/src")],
            transforms: vec![
                derive::request::validate::Transform::default(),
                derive::request::validate::Transform {
                    collection_index: 1,
                    ..Default::default()
                },
            ],
            // The derived collection is never indirected.
            collection: Some(collection("acmeCo/derived")),
            ..Default::default()
        };
        assert_eq!(
            names(indirect.resolved_transforms()),
            [("acmeCo/src".to_string(), Some(0)), ("!".to_string(), None)],
        );
        assert_eq!(indirect.collection.as_ref().unwrap().name, "acmeCo/derived");

        let inline = derive::request::Validate {
            transforms: vec![derive::request::validate::Transform {
                collection: Some(collection("acmeCo/src")),
                ..Default::default()
            }],
            ..Default::default()
        };
        assert_eq!(
            names(inline.resolved_transforms()),
            [("acmeCo/src".to_string(), None)],
        );
    }

    fn keyed(name: &str, key: &[&str]) -> crate::Arc<flow::CollectionSpec> {
        crate::Arc::new(flow::CollectionSpec {
            name: name.to_string(),
            key: key.iter().map(|k| k.to_string()).collect(),
            ..Default::default()
        })
    }

    #[test]
    fn interning_dedups_by_value_and_orders_by_name() {
        let mut interner = Interner::default();

        // Interned in binding order: `zed` first, and `one` twice under
        // differing keys -- as differing `group_by` of one collection produces.
        let zed = keyed("acmeCo/zed", &["/id"]);
        assert_eq!(interner.intern(zed.clone()), 0);
        assert_eq!(interner.intern(keyed("acmeCo/one", &["/a"])), 1);
        assert_eq!(interner.intern(keyed("acmeCo/one", &["/b"])), 2);
        // Shared and value-equal specs both collapse onto their existing entry.
        assert_eq!(interner.intern(zed.clone()), 0);
        assert_eq!(interner.intern(keyed("acmeCo/one", &["/a"])), 1);
        assert_eq!(interner.intern(keyed("acmeCo/mid", &["/id"])), 3);

        let (table, remap) = interner.finish();

        assert_eq!(
            table.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(),
            ["acmeCo/mid", "acmeCo/one", "acmeCo/one", "acmeCo/zed"],
        );
        // Same-name entries keep their interned order.
        assert_eq!(table[1].key, ["/a"]);
        assert_eq!(table[2].key, ["/b"]);
        // `zed` was issued 0 and orders last; `mid` was issued 3 and orders first.
        assert_eq!(remap, [3, 1, 2, 0]);
        // The table shares the interned spec rather than copying it.
        assert!(crate::Arc::ptr_eq(&table[3], &zed));
    }

    #[test]
    fn install_attaches_an_ordered_table() {
        let mut interner = Interner::default();
        let binding = |collection_index| flow::capture_spec::Binding {
            collection_index,
            ..Default::default()
        };
        let mut spec = flow::CaptureSpec {
            bindings: vec![
                binding(interner.intern(collection("acmeCo/zed"))),
                binding(interner.intern(collection("acmeCo/one"))),
                binding(interner.intern(collection("acmeCo/zed"))),
            ],
            inactive_bindings: vec![binding(interner.intern(collection("acmeCo/one")))],
            ..Default::default()
        };
        spec.install(interner);

        // Active and inactive bindings share the one table.
        assert_eq!(
            names(spec.resolved_all_bindings()),
            [
                ("acmeCo/zed".to_string(), Some(1)),
                ("acmeCo/one".to_string(), Some(0)),
                ("acmeCo/zed".to_string(), Some(1)),
                ("acmeCo/one".to_string(), Some(0)),
            ],
        );

        // Installing an empty interner leaves a message in inline form.
        let mut spec = flow::CaptureSpec::default();
        spec.install(Interner::default());
        assert!(spec.linked_collections.is_empty());
    }

    // Assert that `$msg`, which is in inline form, converts to indirect
    // form and back without copying or changing a collection.
    macro_rules! assert_round_trip {
        ($msg:expr, $($bindings:ident),+) => {{
            let inline = $msg;
            let mut msg = inline.clone();

            msg.inline(); // Already inline: a no-op.
            assert_eq!(msg, inline);

            msg.indirect();
            assert!(!msg.linked_collections.is_empty());
            let indirect = msg.clone();
            msg.indirect(); // Already indirect: a no-op.
            assert_eq!(msg, indirect);

            $(for (binding, original) in msg.$bindings.iter().zip(inline.$bindings.iter()) {
                assert!(binding.collection.is_none());

                let entry = msg.linked_collections.get(binding.collection_index as usize);
                match (&original.collection, entry) {
                    (Some(original), Some(entry)) => assert_eq!(original, entry),
                    (None, None) => (),
                    (original, entry) => panic!("{original:?} resolved as {entry:?}"),
                }
            })+
            // Entries are unique, and each shares the allocation of a binding
            // it was interned from.
            for (index, entry) in msg.linked_collections.iter().enumerate() {
                assert!(!msg.linked_collections[..index].contains(entry));
                assert!(
                    [$(&inline.$bindings),+]
                        .iter()
                        .flat_map(|bindings| bindings.iter())
                        .filter_map(|binding| binding.collection.as_ref())
                        .any(|original| crate::Arc::ptr_eq(original, entry))
                );
            }

            msg.inline();
            assert_eq!(msg, inline);
        }};
    }

    #[test]
    fn messages_round_trip_between_forms() {
        let (one, two) = (collection("acmeCo/one"), collection("acmeCo/two"));
        // Equal by value to `one`, but separately allocated.
        let one_copy = collection("acmeCo/one");
        let one_keyed = keyed("acmeCo/one", &["/id"]);

        assert_round_trip!(
            flow::CaptureSpec {
                bindings: [&two, &one, &one_copy]
                    .map(|c| flow::capture_spec::Binding {
                        collection: Some(c.clone()),
                        ..Default::default()
                    })
                    .to_vec(),
                // A binding without a collection doesn't resolve in either form.
                inactive_bindings: vec![
                    flow::capture_spec::Binding::default(),
                    flow::capture_spec::Binding {
                        collection: Some(two.clone()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            bindings,
            inactive_bindings
        );
        assert_round_trip!(
            flow::MaterializationSpec {
                bindings: [&one, &one_keyed, &two, &one]
                    .map(|c| flow::materialization_spec::Binding {
                        collection: Some(c.clone()),
                        ..Default::default()
                    })
                    .to_vec(),
                inactive_bindings: vec![flow::materialization_spec::Binding {
                    collection: Some(one_keyed.clone()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            bindings,
            inactive_bindings
        );
        assert_round_trip!(
            flow::collection_spec::Derivation {
                transforms: [&two, &one]
                    .map(|c| flow::collection_spec::derivation::Transform {
                        collection: Some(c.clone()),
                        ..Default::default()
                    })
                    .to_vec(),
                inactive_transforms: vec![flow::collection_spec::derivation::Transform {
                    collection: Some(one.clone()),
                    ..Default::default()
                }],
                ..Default::default()
            },
            transforms,
            inactive_transforms
        );
        assert_round_trip!(
            capture::request::Validate {
                bindings: [&two, &one, &two]
                    .map(|c| capture::request::validate::Binding {
                        collection: Some(c.clone()),
                        ..Default::default()
                    })
                    .to_vec(),
                ..Default::default()
            },
            bindings
        );
        assert_round_trip!(
            materialize::request::Validate {
                bindings: [&one_keyed, &one]
                    .map(|c| materialize::request::validate::Binding {
                        collection: Some(c.clone()),
                        ..Default::default()
                    })
                    .to_vec(),
                ..Default::default()
            },
            bindings
        );
        assert_round_trip!(
            derive::request::Validate {
                transforms: [&one, &one]
                    .map(|c| derive::request::validate::Transform {
                        collection: Some(c.clone()),
                        ..Default::default()
                    })
                    .to_vec(),
                // The derived collection is never indirected.
                collection: Some(two.clone()),
                ..Default::default()
            },
            transforms
        );
    }
}
