mod common;

use proto_flow::{flow, linked::Resolved};

const INDIRECT_SPECS_YAML: &str = include_str!("indirect_specs.yaml");
const TRANSITION_YAML: &str = include_str!("indirect_specs_transition.yaml");

/// Render a spec's `linked_collections` table, followed by one line per
/// binding naming the table entry it resolves through. Bindings which inline
/// their collection instead render an index of `-`.
fn summarize<'a, B: 'a>(
    table: &[flow::CollectionSpec],
    bindings: impl Iterator<Item = (&'a B, Option<Resolved<'a>>)>,
    label: impl Fn(&B) -> String,
) -> String {
    let mut out = String::new();

    for (index, collection) in table.iter().enumerate() {
        out.push_str(&format!(
            "  [{index}] {} key={:?}\n",
            collection.name, collection.key
        ));
    }
    for (binding, resolved) in bindings {
        let (name, index) = match resolved {
            Some((collection, index)) => (
                collection.name.as_str(),
                index.map_or("-".to_string(), |i| i.to_string()),
            ),
            None => ("!unresolved", "-".to_string()),
        };
        out.push_str(&format!("  {} => [{index}] {name}\n", label(binding)));
    }
    out
}

fn summarize_all(outcome: &common::Outcome) -> String {
    let mut out = String::new();

    for row in outcome.built_captures.iter() {
        let spec = row.spec.as_ref().unwrap();
        out.push_str(&format!("capture {}:\n", row.capture));
        out.push_str(&summarize(
            &spec.linked_collections,
            spec.resolved_all_bindings(),
            |binding| format!("{:?}", binding.resource_path),
        ));
    }
    for row in outcome.built_collections.iter() {
        let Some(derivation) = row.spec.as_ref().and_then(|s| s.derivation.as_ref()) else {
            continue;
        };
        out.push_str(&format!("derivation {}:\n", row.collection));
        out.push_str(&summarize(
            &derivation.linked_collections,
            derivation.resolved_all_transforms(),
            |transform| transform.name.clone(),
        ));
    }
    for row in outcome.built_materializations.iter() {
        let spec = row.spec.as_ref().unwrap();
        out.push_str(&format!("materialization {}:\n", row.materialization));
        out.push_str(&summarize(
            &spec.linked_collections,
            spec.resolved_all_bindings(),
            |binding| format!("{:?}", binding.resource_path),
        ));
    }
    out
}

/// A flagged task interns its collections once each: many bindings over few
/// collections collapse onto a table ordered by collection name, and a
/// materialization's `group_by` rewrites produce distinct same-name entries.
#[test]
fn test_flagged_tasks_build_indirect_specs() {
    let outcome = common::run(INDIRECT_SPECS_YAML, "{}");
    assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);

    insta::assert_snapshot!(summarize_all(&outcome));
}

/// A capture binds its target unmodified, so bindings of one collection share
/// a single resolution of it. They must nonetheless keep their own per-binding
/// state: a shared resolution is a shared *collection*, not a shared binding.
#[test]
fn test_shared_targets_keep_per_binding_state() {
    let outcome = common::run(
        INDIRECT_SPECS_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    acmeCo/capture:
      bindings:
        - target: acmeCo/one
          resource: { _meta: { path: [one, a] }, source: one-a }
          backfill: 7
        - target: acmeCo/two
          resource: { _meta: { path: [two, a] }, source: two-a }
        - target: acmeCo/one
          resource: { _meta: { path: [one, b] }, source: one-b }
          backfill: 3
        - target: acmeCo/one
          resource: { _meta: { path: [one, c] }, source: one-c }
"#,
    );
    assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);

    let spec = outcome.built_captures[0].spec.as_ref().unwrap();

    // Three bindings of `acmeCo/one` share one table entry: the shared
    // resolution guarantees value-identical specs, which the interner's
    // by-value de-duplication then collapses.
    assert_eq!(spec.linked_collections.len(), 2);

    let summary: Vec<String> = spec
        .resolved_bindings()
        .map(|(binding, resolved)| {
            let (collection, index) = resolved.unwrap();
            format!(
                "{:?} backfill={} state_key={} resource={} => [{}] {}",
                binding.resource_path,
                binding.backfill,
                binding.state_key,
                std::str::from_utf8(&binding.resource_config_json).unwrap(),
                index.unwrap(),
                collection.name,
            )
        })
        .collect();

    insta::assert_debug_snapshot!(summary);
}

/// Clearing the flag re-inlines every collection, yielding the encoding which
/// predates indirect specs. This is the round-trip of interning and inlining.
#[test]
fn test_unflagged_tasks_build_inline_specs() {
    let outcome = common::run(
        INDIRECT_SPECS_YAML,
        r#"
test://example/catalog.yaml:
  collections:
    acmeCo/derived:
      derive:
        shards: { flags: null }
  captures:
    acmeCo/capture:
      shards: { flags: null }
  materializations:
    acmeCo/materialization:
      shards: { flags: null }
"#,
    );
    assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);

    insta::assert_snapshot!(summarize_all(&outcome));
}

/// The flag is an end-to-end assertion whose only valid value is "true":
/// spelling it any other way is an error, not a silent no-op.
#[test]
fn test_invalid_flag_value_errors() {
    let errors = common::run_errors(
        INDIRECT_SPECS_YAML,
        r#"
test://example/catalog.yaml:
  captures:
    acmeCo/capture:
      shards: { flags: { indirect-specs: "ture" } }
  materializations:
    acmeCo/materialization:
      shards: { flags: { indirect-specs: "false" } }
"#,
    );
    insta::assert_debug_snapshot!(errors);
}

/// Inactive bindings carry over from the live spec in whichever form it
/// arrived in, and are re-interned (or re-inline) into the spec being built.
/// An index of the live table is never copied across generations.
#[test]
fn test_inactive_bindings_across_a_flag_transition() {
    // Live specs in indirect form, drafted without the flag: inline.
    let outcome = common::run(TRANSITION_YAML, "{}");
    assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);
    insta::assert_snapshot!("indirect_live_unflagged_draft", summarize_all(&outcome));

    // Live specs in inline form, drafted with the flag: intern.
    let outcome = common::run(
        TRANSITION_YAML,
        r#"
driver:
  liveCaptures:
    acmeCo/capture: { indirectSpecs: false }
  liveMaterializations:
    acmeCo/materialization: { indirectSpecs: false }
test://example/catalog.yaml:
  captures:
    acmeCo/capture:
      shards: { flags: { indirect-specs: "true" } }
  materializations:
    acmeCo/materialization:
      shards: { flags: { indirect-specs: "true" } }
"#,
    );
    assert!(outcome.errors.is_empty(), "{:?}", outcome.errors);
    insta::assert_snapshot!("inline_live_flagged_draft", summarize_all(&outcome));
}
