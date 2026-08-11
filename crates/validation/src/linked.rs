//! The indirect-encoding writer: an interner over `flow::CollectionSpec`, plus a
//! per-message step which installs a finished interner into its message.
//!
//! Builders in this crate always construct specs and Validate requests in indirect
//! form, interning each binding's collection as the binding is built. The
//! `indirect-specs` shard flag then gates a single post-process: when it's unset,
//! `install_*` *inlines* the message instead -- re-inlining each binding's
//! collection and dropping the table -- reproducing byte-for-byte the encoding
//! which predates indirect specs.
//!
//! Inlining is writer-internal and transitional. It does not soften the
//! read-side rule that a spec keeps whichever form it arrived in: readers go
//! through the accessors of `proto_flow::linked` and never normalize. Once
//! indirect form is universal, the `indirect_specs` argument and its branch are
//! deleted, and what remains is already the end-state builder.

use proto_flow::{capture, derive, flow, materialize};
use std::collections::HashMap;

/// Interns the collections of a single spec-carrying message into its
/// `linked_collections` table, collapsing bindings which share a collection.
///
/// Entries are unique by *value*, not by name: two bindings of one collection
/// whose `group_by` differs carry differing `key` and `projections`, and are
/// distinct entries which happen to share a name.
#[derive(Default)]
pub struct Interner {
    specs: Vec<flow::CollectionSpec>,
    // Candidate indices into `specs` for each collection name. Names are
    // rarely duplicated, so a linear equality scan over candidates is cheap.
    by_name: HashMap<String, Vec<u32>>,
}

impl Interner {
    /// Intern an owned spec, returning its issued index.
    pub fn intern(&mut self, spec: flow::CollectionSpec) -> u32 {
        match self.lookup(&spec) {
            Some(index) => index,
            None => self.push(spec),
        }
    }

    /// Intern a borrowed spec, cloning it only if it's not already interned.
    pub fn intern_ref(&mut self, spec: &flow::CollectionSpec) -> u32 {
        match self.lookup(spec) {
            Some(index) => index,
            None => self.push(spec.clone()),
        }
    }

    fn lookup(&self, spec: &flow::CollectionSpec) -> Option<u32> {
        self.by_name
            .get(&spec.name)?
            .iter()
            .copied()
            .find(|index| self.specs[*index as usize] == *spec)
    }

    fn push(&mut self, spec: flow::CollectionSpec) -> u32 {
        let index = self.specs.len() as u32;
        self.by_name
            .entry(spec.name.clone())
            .or_default()
            .push(index);
        self.specs.push(spec);
        index
    }

    /// Yield the finished table, ordered by collection name, alongside a remap
    /// from each index issued by `intern` into the ordered table.
    ///
    /// Interning happens in binding order while the table is conventionally
    /// ordered by name, and the remap reconciles the two. The ordering is
    /// stable, so entries sharing a name keep their interned order.
    fn finish(self) -> (Vec<flow::CollectionSpec>, Vec<u32>) {
        let Interner { specs, by_name: _ } = self;

        let mut order: Vec<u32> = (0..specs.len() as u32).collect();
        order.sort_by(|l, r| specs[*l as usize].name.cmp(&specs[*r as usize].name));

        let mut remap = vec![0; specs.len()];
        for (ordered, issued) in order.iter().enumerate() {
            remap[*issued as usize] = ordered as u32;
        }

        let mut specs: Vec<Option<flow::CollectionSpec>> = specs.into_iter().map(Some).collect();
        let table = order
            .iter()
            .map(|issued| specs[*issued as usize].take().unwrap())
            .collect();

        (table, remap)
    }
}

/// Define `$install`, which installs `interner` into the `$bindings` lists of
/// `$msg`. Each binding must carry an index issued by `interner`.
macro_rules! install {
    ($install:ident, $msg:ty, $($bindings:ident),+) => {
        /// Resolve the issued `collection_index` of each binding against
        /// `interner`. With `indirect_specs`, the interned table is attached and
        /// indices are remapped into it. Otherwise the message is inline:
        /// each binding re-inlines its own collection and no table is attached.
        pub fn $install(msg: &mut $msg, interner: Interner, indirect_specs: bool) {
            let (table, remap) = interner.finish();

            $(for binding in msg.$bindings.iter_mut() {
                let index = remap[binding.collection_index as usize];

                if indirect_specs {
                    binding.collection_index = index;
                } else {
                    binding.collection = Some(table[index as usize].clone());
                    binding.collection_index = 0;
                }
            })+

            if indirect_specs {
                msg.linked_collections = table;
            }
        }
    };
}

install!(
    install_capture_spec,
    flow::CaptureSpec,
    bindings,
    inactive_bindings
);
install!(
    install_materialization_spec,
    flow::MaterializationSpec,
    bindings,
    inactive_bindings
);
install!(
    install_derivation,
    flow::collection_spec::Derivation,
    transforms,
    inactive_transforms
);
install!(
    install_capture_validate,
    capture::request::Validate,
    bindings
);
install!(
    install_materialize_validate,
    materialize::request::Validate,
    bindings
);
install!(
    install_derive_validate,
    derive::request::Validate,
    transforms
);

#[cfg(test)]
mod test {
    use super::*;

    fn collection(name: &str, key: &[&str]) -> flow::CollectionSpec {
        flow::CollectionSpec {
            name: name.to_string(),
            key: key.iter().map(|k| k.to_string()).collect(),
            ..Default::default()
        }
    }

    fn binding(collection_index: u32) -> flow::capture_spec::Binding {
        flow::capture_spec::Binding {
            collection_index,
            ..Default::default()
        }
    }

    #[test]
    fn interning_dedups_by_value_and_orders_by_name() {
        let mut interner = Interner::default();

        // Interned in binding order: `zed` first, and `one` twice under
        // differing keys -- as differing `group_by` of one collection produces.
        assert_eq!(interner.intern(collection("acmeCo/zed", &["/id"])), 0);
        assert_eq!(interner.intern(collection("acmeCo/one", &["/a"])), 1);
        assert_eq!(interner.intern(collection("acmeCo/one", &["/b"])), 2);
        // Value-equal specs collapse onto their existing entry.
        assert_eq!(interner.intern(collection("acmeCo/one", &["/a"])), 1);
        assert_eq!(interner.intern_ref(&collection("acmeCo/zed", &["/id"])), 0);
        assert_eq!(interner.intern_ref(&collection("acmeCo/mid", &["/id"])), 3);

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
    }

    #[test]
    fn install_attaches_a_table_when_flagged() {
        let mut interner = Interner::default();
        let mut spec = flow::CaptureSpec {
            bindings: vec![
                binding(interner.intern(collection("acmeCo/zed", &[]))),
                binding(interner.intern(collection("acmeCo/one", &[]))),
                binding(interner.intern(collection("acmeCo/zed", &[]))),
            ],
            inactive_bindings: vec![binding(interner.intern_ref(&collection("acmeCo/one", &[])))],
            ..Default::default()
        };
        install_capture_spec(&mut spec, interner, true);

        assert_eq!(
            spec.linked_collections
                .iter()
                .map(|c| c.name.as_str())
                .collect::<Vec<_>>(),
            ["acmeCo/one", "acmeCo/zed"],
        );
        // Active and inactive bindings share the one table, and resolve
        // through the accessors of `proto_flow::linked`.
        assert_eq!(
            spec.resolved_all_bindings()
                .map(|(_binding, resolved)| resolved.unwrap().0.name.as_str())
                .collect::<Vec<_>>(),
            ["acmeCo/zed", "acmeCo/one", "acmeCo/zed", "acmeCo/one"],
        );
        assert!(spec.bindings.iter().all(|b| b.collection.is_none()));
    }

    #[test]
    fn install_inlines_when_unflagged() {
        let mut interner = Interner::default();
        let mut spec = flow::CaptureSpec {
            bindings: vec![
                binding(interner.intern(collection("acmeCo/zed", &[]))),
                binding(interner.intern(collection("acmeCo/one", &[]))),
                binding(interner.intern(collection("acmeCo/zed", &[]))),
            ],
            inactive_bindings: vec![binding(interner.intern_ref(&collection("acmeCo/one", &[])))],
            ..Default::default()
        };
        install_capture_spec(&mut spec, interner, false);

        assert!(spec.linked_collections.is_empty());
        assert_eq!(
            spec.resolved_all_bindings()
                .map(|(_binding, resolved)| resolved.unwrap().0.name.as_str())
                .collect::<Vec<_>>(),
            ["acmeCo/zed", "acmeCo/one", "acmeCo/zed", "acmeCo/one"],
        );
        // Inlining is the exact inverse of interning: each binding carries
        // back the value it interned, in unchanged binding order.
        assert!(spec.bindings.iter().all(|b| b.collection_index == 0));
    }

    #[test]
    fn install_of_an_empty_interner_is_a_no_op() {
        let mut spec = flow::CaptureSpec::default();
        install_capture_spec(&mut spec, Interner::default(), true);
        assert!(spec.linked_collections.is_empty());
    }

    #[test]
    fn transforms_and_validate_requests_install_alike() {
        let mut interner = Interner::default();
        let mut derivation = flow::collection_spec::Derivation {
            transforms: vec![flow::collection_spec::derivation::Transform {
                collection_index: interner.intern(collection("acmeCo/src", &[])),
                ..Default::default()
            }],
            inactive_transforms: vec![flow::collection_spec::derivation::Transform {
                collection_index: interner.intern(collection("acmeCo/gone", &[])),
                ..Default::default()
            }],
            ..Default::default()
        };
        install_derivation(&mut derivation, interner, true);

        assert_eq!(
            derivation
                .resolved_all_transforms()
                .map(|(_t, resolved)| resolved.unwrap().0.name.as_str())
                .collect::<Vec<_>>(),
            ["acmeCo/src", "acmeCo/gone"],
        );

        let mut interner = Interner::default();
        let mut request = materialize::request::Validate {
            bindings: vec![
                materialize::request::validate::Binding {
                    collection_index: interner.intern(collection("acmeCo/two", &[])),
                    ..Default::default()
                },
                materialize::request::validate::Binding {
                    collection_index: interner.intern(collection("acmeCo/one", &[])),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        install_materialize_validate(&mut request, interner, true);

        assert_eq!(
            request
                .resolved_bindings()
                .map(|(_b, resolved)| resolved.unwrap().1.unwrap())
                .collect::<Vec<_>>(),
            [1, 0],
        );
    }
}
