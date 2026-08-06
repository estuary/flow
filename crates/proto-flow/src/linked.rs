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
//! Nothing here normalizes in either direction: a spec keeps whichever form it
//! arrived in, and readers go through these accessors instead of caring.

use crate::{capture, derive, flow, materialize};

/// A resolved collection paired with its identity within the parent message.
///
/// The identity is `Some(index)` into the parent's `linked_collections` when
/// the parent is in indirect form, and `None` when the binding inlines its own
/// collection -- in which case the binding *is* the identity. Callers which
/// memoize per-collection derived state (validators, extractors, projections)
/// key on this, falling back to per-binding state when it's `None`.
pub type Resolved<'a> = (&'a flow::CollectionSpec, Option<u32>);

/// Resolve one binding's collection against its parent's table.
///
/// Returns None if an indirect-form `collection_index` is out of bounds, or if an
/// inline-form binding is missing its collection. Both are malformed
/// messages, and returning None lets call sites attach their own context
/// rather than panicking here.
fn resolve<'a>(
    linked_collections: &'a [flow::CollectionSpec],
    inline: Option<&'a flow::CollectionSpec>,
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

#[cfg(test)]
mod test {
    use super::*;

    fn collection(name: &str) -> flow::CollectionSpec {
        flow::CollectionSpec {
            name: name.to_string(),
            ..Default::default()
        }
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
        keyed.key = vec!["/id".to_string()];

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
}
