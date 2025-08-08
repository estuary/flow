use itertools::Itertools;
use json::schema::types;
use materialize::response::validated::constraint::Type as ConstraintType;
use proto_flow::{flow, materialize};
use std::collections::BTreeMap;
use tables::EitherOrBoth as EOB;

/// Select is a rationale for including a field in selection.
#[derive(
    thiserror::Error,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum Select {
    #[error("field is within the desired depth")]
    DesiredDepth,
    #[error("field is important metadata which is typically selected")]
    CoreMetadata,
    #[error("field's location is required by the connector ({reason})")]
    ConnectorRequiresLocation { reason: String },
    #[error("field is a user-defined projection")]
    UserDefined,
    #[error("field is part of the current materialization")]
    CurrentValue, // <- order means we walk before a DesiredDepth parent.
    #[error("field is a partition key of the collection")]
    PartitionKey,
    #[error("field is required by the connector ({reason})")]
    ConnectorRequires { reason: String },
    #[error("field is required by the user's field selection")]
    UserRequires,
    #[error("field is currently used to store the document")]
    CurrentDocument,
    #[error("field is part of the materialization group-by key")]
    GroupByKey,
}

/// Reject is a rationale for rejecting a field from selection.
#[derive(
    thiserror::Error,
    Debug,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Clone,
    serde::Serialize,
    serde::Deserialize,
)]
pub enum Reject {
    #[error("field doesn't meet any selection criteria")]
    NotSelected,
    #[error("field's location is underneath another selected field")]
    CoveredLocation,
    #[error("field's parent location is excluded by the user's field selection")]
    ExcludedParent,
    #[error("field's location is already materialized by another selected field")]
    DuplicateLocation,
    #[error("field is represented by the endpoint as {folded_field:?}, which is ambiguous with another selected field")]
    DuplicateFold { folded_field: String },
    #[error("connector didn't return a constraint for this field")]
    ConnectorOmits,
    #[error("field does not exist within the source collection")]
    CollectionOmits,
    #[error("connector cannot support this field without a back-fill ({reason})")]
    ConnectorIncompatible { reason: String },
    #[error("field is forbidden by the connector ({reason})")]
    ConnectorForbids { reason: String },
    #[error("field is excluded by the user's field selection")]
    UserExcludes,
}

/// Conflict is a conflict between a Select and a Reject for the same field.
#[derive(thiserror::Error, Debug)]
#[error("conflict for field {field:?}: {select:#} but {reject:#}")]
pub struct Conflict {
    pub field: String,
    pub select: Select,
    pub reject: Reject,
}

/// Evaluate field selection for a materialization binding, returning the outcome
/// and any conflicts. If all conflicts are Reject::ConnectorIncompatible,
/// then the returned FieldSelection is valid if a back-fill is also performed.
pub fn evaluate(
    collection_projections: &[flow::Projection],
    group_by: Vec<String>,
    live_spec: Option<&flow::materialization_spec::Binding>,
    model: &models::MaterializationBinding,
    validated_constraints: &BTreeMap<String, materialize::response::validated::Constraint>,
) -> (
    (flow::FieldSelection, Vec<Conflict>),
    BTreeMap<String, EOB<Select, Reject>>,
) {
    let models::MaterializationBinding {
        fields: model_fields,
        backfill: model_backfill,
        ..
    } = model;

    // If we intend to back-fill, then live fields have no effect on selection.
    let live_field_selection = match live_spec {
        Some(live) if live.backfill == *model_backfill => live.field_selection.as_ref(),
        _ => None,
    };

    let (selects, rejects, field_config) = extract_constraints(
        collection_projections,
        &group_by,
        live_field_selection,
        model_fields,
        validated_constraints,
    );
    let (document_field, field_outcomes) = group_outcomes(
        collection_projections,
        rejects,
        selects,
        validated_constraints,
    );

    // TODO(johnny): `field_outcomes` isn't intended to be part of the function return,
    // but is here for now in support of validation via shadow difference logging.
    (
        build_selection(
            group_by,
            document_field,
            field_config,
            field_outcomes.clone(),
        ),
        field_outcomes,
    )
}

/// Map all applicable sources of field selection constraints into Select and Reject.
/// Results are ordered by field name, then by descending Select/Reject rank.
pub fn extract_constraints<'a>(
    collection_projections: &'a [flow::Projection],
    group_by: &'a [String],
    live_field_selection: Option<&'a flow::FieldSelection>,
    model_fields: &'a models::MaterializationFields,
    validated_constraints: &'a BTreeMap<String, materialize::response::validated::Constraint>,
) -> (
    Vec<(&'a str, Select)>,
    Vec<(&'a str, Reject)>,
    BTreeMap<String, String>,
) {
    let models::MaterializationFields {
        group_by: _,
        require,
        exclude,
        recommended,
    } = model_fields;

    let desired_depth = match recommended {
        models::RecommendedDepth::Bool(false) => 0,
        models::RecommendedDepth::Usize(depth) => *depth,
        models::RecommendedDepth::Bool(true) => usize::MAX,
    };

    let mut selects: Vec<(&str, Select)> = Vec::new();
    let mut rejects: Vec<(&str, Reject)> = Vec::new();
    let mut field_config: BTreeMap<String, String> = BTreeMap::new();

    // Group-by keys are always required.
    for field in group_by {
        selects.push((field.as_str(), Select::GroupByKey));
    }

    // If there's a live selection, it drives Select constraints which prefer stability.
    if let Some(live) = live_field_selection {
        if !live.document.is_empty() {
            selects.push((live.document.as_str(), Select::CurrentDocument));
        }
        for field in live.values.iter() {
            selects.push((field.as_str(), Select::CurrentValue));
        }
    }

    // Account for fields required or excluded by the user's model.
    for (field, config) in require {
        selects.push((field.as_str(), Select::UserRequires));
        field_config.insert(field.to_string(), config.to_string());
    }
    for field in exclude {
        rejects.push((field.as_str(), Reject::UserExcludes));
    }

    // Walk projections on ascending JSON pointer (parents order before children).
    let mut projections_ptr_it = collection_projections
        .iter()
        .sorted_by(|l, r| l.ptr.cmp(&r.ptr))
        .peekable();

    // Map projections into Select constraints.
    while let Some(p) = projections_ptr_it.next() {
        if p.is_partition_key {
            selects.push((p.field.as_str(), Select::PartitionKey));
        }
        if p.explicit {
            selects.push((p.field.as_str(), Select::UserDefined));
        }

        // Certain metadata fields have special treatment if there's _any_ desired selection:
        // - flow_published_at is /_meta/uuid but mapped to extract a date-time
        // - _meta/op is core.
        if desired_depth > 0 && ["flow_published_at", "_meta/op"].contains(&p.field.as_str()) {
            selects.push((p.field.as_str(), Select::CoreMetadata));
        }

        // Determine the number of JSON pointer path components.
        let depth = doc::Pointer::from_str(&p.ptr).iter().count();

        let desired = if p.ptr.starts_with("/_meta") {
            false
        } else if depth == desired_depth {
            true
        } else if depth > desired_depth {
            false
        } else if ![types::OBJECT, types::OBJECT | types::NULL].contains(
            &p.inference
                .as_ref()
                .map(|inf| types::Set::from_iter(&inf.types))
                .unwrap_or(types::INVALID),
        ) {
            true // Desire below-target locations which aren't objects.
        } else if !projections_ptr_it
            .peek()
            .map(|next| is_parent_of(&p.ptr, &next.ptr))
            .unwrap_or_default()
            && !matches!(recommended, models::RecommendedDepth::Bool(true))
        {
            // Desire objects that have no projected children.
            // The `recommended != true` constraint has no intrinsic rationale:
            // it's a compromise to avoid churning legacy field selection behavior.
            true
        } else {
            false // Omit below-depth objects with children.
        };

        if desired {
            selects.push((p.field.as_str(), Select::DesiredDepth));
        };
    }

    // Finally, map Validated constraints into Select and Reject.
    for (field, constraint) in validated_constraints.iter() {
        match ConstraintType::try_from(constraint.r#type) {
            Ok(ConstraintType::FieldRequired) => selects.push((
                field,
                Select::ConnectorRequires {
                    reason: constraint.reason.clone(),
                },
            )),
            Ok(ConstraintType::LocationRequired) => selects.push((
                field,
                Select::ConnectorRequiresLocation {
                    reason: constraint.reason.clone(),
                },
            )),
            Ok(ConstraintType::FieldForbidden) => rejects.push((
                field,
                Reject::ConnectorForbids {
                    reason: constraint.reason.clone(),
                },
            )),
            Ok(ConstraintType::Incompatible | ConstraintType::Unsatisfiable)
                if live_field_selection.is_some() =>
            {
                rejects.push((
                    field,
                    Reject::ConnectorIncompatible {
                        reason: constraint.reason.clone(),
                    },
                ))
            }
            Ok(
                ConstraintType::LocationRecommended
                | ConstraintType::FieldOptional
                | ConstraintType::Incompatible
                | ConstraintType::Unsatisfiable,
            ) => {
                // Field is neither selected nor rejected by the connector.
                // Note: UNSATISFIABLE is an alias for INCOMPATIBLE and treated the same way.
            }

            // Any other constraint type is invalid and errors elsewhere.
            Ok(ConstraintType::Invalid) | Err(_) => {}
        };
    }

    // Order by field name, then by descending Select/Reject rank.
    selects.sort_by(|l, r| l.0.cmp(r.0).then(l.1.cmp(&r.1).reverse()));
    rejects.sort_by(|l, r| l.0.cmp(r.0).then(l.1.cmp(&r.1).reverse()));

    (selects, rejects, field_config)
}

/// Group Select and Reject outcomes by field name and apply depth and field-fold
/// constraints, returning a selected document field and per-field outcomes.
pub fn group_outcomes(
    collection_projections: &[flow::Projection],
    rejects: Vec<(&str, Reject)>,
    selects: Vec<(&str, Select)>,
    validated_constraints: &BTreeMap<String, materialize::response::validated::Constraint>,
) -> (
    Option<String>,                        // Document field.
    BTreeMap<String, EOB<Select, Reject>>, // Field outcomes.
) {
    // Projections are supposed to be ordered and unique by field name,
    // but they're read from network or we may be in a WASM context, so be safe.
    let projections = collection_projections
        .iter()
        .sorted_by(|l, r| l.field.cmp(&r.field))
        .dedup_by(|l, r| l.field == r.field);

    // `selects` and `rejects` are ordered by field name, and then by descending Select/Reject rank.
    // Outer join to the top-rank Select or Reject for each field name.
    let grouped = itertools::merge_join_by(
        selects.into_iter().dedup_by(|(l, _), (r, _)| l == r),
        rejects.into_iter().dedup_by(|(l, _), (r, _)| l == r),
        |(l, _), (r, _)| l.cmp(r),
    )
    .map(|eob| match eob {
        EOB::Left((field, select)) => (field, Some(select), None),
        EOB::Both((field, select), (_, reject)) => (field, Some(select), Some(reject)),
        EOB::Right((field, reject)) => (field, None, Some(reject)),
    });

    // Next, outer join with projections.
    let grouped = itertools::merge_join_by(grouped, projections, |(l, _, _), r| {
        (*l).cmp(r.field.as_str())
    })
    .map(|eob| match eob {
        EOB::Left((field, select, reject)) => (field, select, reject, None),
        EOB::Both((field, select, reject), projection) => (field, select, reject, Some(projection)),
        EOB::Right(projection) => (projection.field.as_str(), None, None, Some(projection)),
    });

    // Finally, outer join with connector constraints.
    let grouped = itertools::merge_join_by(
        grouped,
        validated_constraints.iter(),
        |(l, _, _, _), (r, _)| (*l).cmp(r.as_str()),
    )
    .map(|eob| match eob {
        EOB::Left((field, select, reject, projection)) => (field, select, reject, projection, None),
        EOB::Both((field, select, reject, projection), (_, constraint)) => {
            (field, select, reject, projection, Some(constraint))
        }
        EOB::Right((field, constraint)) => (field.as_str(), None, None, None, Some(constraint)),
    });

    // Re-order on descending Select priority, and materialize to a Vec.
    let grouped: Vec<(
        &str, // Field name.
        Option<Select>,
        Option<Reject>,
        Option<&flow::Projection>,
        Option<&materialize::response::validated::Constraint>,
    )> = grouped
        .sorted_by(|(_, l, _, _, _), (_, r, _, _, _)| l.cmp(r).reverse())
        .collect();

    // Pre-scan to find user-excluded canonical projections.
    let mut excluded_canonical_ptrs: Vec<&str> = Vec::new();
    for (field, _, reject, projection, _) in &grouped {
        if matches!(reject, Some(Reject::UserExcludes)) {
            if let Some(projection) = projection {
                // Check if this is a canonical projection (field matches ptr without leading '/').
                if projection.ptr.len() > 1 && &projection.ptr[1..] == *field {
                    excluded_canonical_ptrs.push(projection.ptr.as_str());
                }
            }
        }
    }

    let mut document_field: Option<String> = None;
    let mut outcomes: BTreeMap<String, EOB<Select, Reject>> = BTreeMap::new();
    let mut selected_folds: Vec<&str> = Vec::new();
    let mut selected_ptrs: Vec<&str> = Vec::new();

    for (field, mut select, mut reject, projection, constraint) in grouped {
        // Unwrap `projection` to its JSON pointer location.
        let field_ptr = if let Some(projection) = projection {
            projection.ptr.as_str()
        } else {
            reject = reject.max(Some(Reject::CollectionOmits));
            ""
        };

        // Unwrap `constraint` to its folded field, or the original field if not provided.
        let folded_field = if let Some(constraint) = constraint {
            if constraint.folded_field.is_empty() {
                field
            } else {
                constraint.folded_field.as_str()
            }
        } else {
            reject = reject.max(Some(Reject::ConnectorOmits));
            field
        };

        // Does the field fold collide with an already-selected value?
        if selected_folds.contains(&folded_field) {
            reject = reject.max(Some(Reject::DuplicateFold {
                folded_field: folded_field.to_string(),
            }));
        }

        // Does the field pointer exactly equal that of another selected field?
        if selected_ptrs.contains(&field_ptr) {
            if matches!(
                select,
                None | Some(
                    Select::CoreMetadata
                        | Select::DesiredDepth
                        | Select::ConnectorRequiresLocation { .. }
                )
            ) {
                select = None; // Satisfied by the already-selected field.
                reject = reject.max(Some(Reject::DuplicateLocation));
            }
        }
        // Is the field pointer a child of another selected field?
        else if selected_ptrs
            .iter()
            .any(|selected_ptr| is_parent_of(selected_ptr, field_ptr))
        {
            if matches!(select, None | Some(Select::DesiredDepth)) {
                select = None; // Satisfied by the already-selected field.
                reject = reject.max(Some(Reject::CoveredLocation));
            }
        }
        // Is the field pointer a child of an excluded canonical projection?
        else if excluded_canonical_ptrs
            .iter()
            .any(|excluded_ptr| is_parent_of(excluded_ptr, field_ptr))
        {
            if matches!(select, None | Some(Select::DesiredDepth)) {
                select = None; // Override DesiredDepth selection.
                reject = reject.max(Some(Reject::ExcludedParent));
            }
        }

        let outcome = match (select, reject) {
            (None, None) => EOB::Right(Reject::NotSelected),
            (None, Some(reject)) => EOB::Right(reject),
            (Some(select), None) => EOB::Left(select),

            // Always surface incompatible conflicts for any Select reason.
            (Some(select), Some(Reject::ConnectorIncompatible { reason })) => {
                EOB::Both(select, Reject::ConnectorIncompatible { reason })
            }
            // Certain Select reasons may be be overridden by remaining Reject reasons.
            (
                Some(
                    Select::DesiredDepth
                    | Select::CoreMetadata
                    | Select::UserDefined
                    | Select::CurrentValue
                    | Select::PartitionKey,
                ),
                Some(reject),
            ) => EOB::Right(reject),

            // Remaining cases are conflicts and produce errors.
            (Some(select), Some(reject)) => EOB::Both(select, reject),
        };

        if let EOB::Left(_select) = &outcome {
            // Track selected fold and pointer for subsequent evaluations.
            if field_ptr != "" {
                selected_ptrs.push(field_ptr);
            }
            // Track picked document field.
            else if document_field.is_none() {
                document_field = Some(field.to_string());
            }
            selected_folds.push(folded_field);
        }

        outcomes.insert(field.to_string(), outcome);
    }

    (document_field, outcomes)
}

/// Build a flow::FieldSelection from the grouped outcomes, capturing conflicts.
pub fn build_selection(
    group_by: Vec<String>,
    document_field: Option<String>,
    field_config: BTreeMap<String, String>,
    field_outcomes: BTreeMap<String, EOB<Select, Reject>>,
) -> (flow::FieldSelection, Vec<Conflict>) {
    let mut conflicts = Vec::new();
    let mut values = Vec::new();

    for (field, outcome) in field_outcomes {
        let _select: Select =
            if let EOB::Both(select, Reject::ConnectorIncompatible { reason }) = outcome {
                // Incompatible means the field *would* be FieldOptional if we
                // backfill, but is currently incompatible. Record the conflict,
                // but also produce a field selection which presumes a backfill.
                conflicts.push(Conflict {
                    field: field.clone(),
                    select: select.clone(),
                    reject: Reject::ConnectorIncompatible { reason },
                });
                select
            } else if let EOB::Both(select, reject) = outcome {
                conflicts.push(Conflict {
                    field,
                    select,
                    reject,
                });
                continue;
            } else if let EOB::Left(select) = outcome {
                select
            } else {
                continue; // EOB::Right(reject).
            };

        if Some(&field) == document_field.as_ref() {
            // Captured as `document` and not in `values`.
        } else if group_by.contains(&field) {
            // Captured in `keys` and not in `values`.
        } else {
            values.push(field); // Everything else is a value.
        }
    }

    (
        flow::FieldSelection {
            keys: group_by,
            values,
            document: document_field.unwrap_or_default(),
            field_config_json_map: field_config,
        },
        conflicts,
    )
}

fn is_parent_of(ptr: &str, other: &str) -> bool {
    other.len() > ptr.len()
        && ptr
            .chars()
            .chain(std::iter::once('/'))
            .zip(other.chars())
            .all(|(a, b)| a == b)
}

#[cfg(test)]
mod tests {

    use super::{build_selection, extract_constraints, group_outcomes, Reject, Select};
    use proto_flow::{flow, materialize};
    use std::collections::BTreeMap;

    #[derive(serde::Deserialize)]
    struct Fixture {
        collection: models::CollectionDef,
        model: models::MaterializationFields,
        validated: BTreeMap<String, materialize::response::validated::Constraint>,
        live: Option<flow::FieldSelection>,
    }

    #[derive(Debug)]
    #[allow(dead_code)]
    struct Snap {
        selects: Vec<(String, Select)>,
        rejects: Vec<(String, Reject)>,
        group_by: Vec<String>,
        document: Option<String>,
        field_outcomes: BTreeMap<String, tables::EitherOrBoth<Select, Reject>>,
        selection: flow::FieldSelection,
        conflicts: Vec<String>,
    }

    #[test]
    fn test_vanilla() {
        let snap = run_test(include_str!("field_selection.fixture.yaml"), "{}");
        insta::assert_debug_snapshot!(snap);
    }

    #[test]
    fn test_no_live_alt_group_by() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
live: null
model:
    groupBy: [AnInt, ABool]
"##,
        );
        insta::assert_debug_snapshot!(snap);
    }

    #[test]
    fn test_no_live_no_projections_depth_0() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
collection:
    projections: null
model:
    recommended: false
    require:
        an_int: {key: config} # Expect this is passed through.
live: null
"##,
        );
        insta::assert_debug_snapshot!(snap);
    }

    #[test]
    fn test_no_live_no_projections_depth_1() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
collection:
    projections: null
model:
    recommended: 1
live: null
"##,
        );
        insta::assert_debug_snapshot!(snap);
    }

    #[test]
    fn test_no_live_no_projections_depth_2_with_delta_model_group_by() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
collection:
    projections: null
model:
    recommended: 2
    groupBy: [nested/baz, a_bool]
validated:
    flow_document: { type: FIELD_OPTIONAL } # Not LOCATION_REQUIRED.
live: null
"##,
        );
        insta::assert_debug_snapshot!(snap);
    }

    #[test]
    fn test_no_live_top_level_locations_required_alt_group_by() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
model:
    groupBy: [AnInt, ABool]
validated:
    _meta: { type: LOCATION_REQUIRED }
    a_bool: { type: LOCATION_REQUIRED }
    a_date_time: { type: LOCATION_REQUIRED }
    a_map: { type: LOCATION_REQUIRED }
    a_num1: { type: LOCATION_REQUIRED }
    an_array: { type: LOCATION_REQUIRED }
    an_int: { type: LOCATION_REQUIRED }
    flow_document: { type: FIELD_OPTIONAL } # Was LOCATION_REQUIRED.
    nested: { type: LOCATION_REQUIRED }
live: null
"##,
        );
        insta::assert_debug_snapshot!(snap);
    }

    #[test]
    fn test_no_live_no_projections_no_require_mixed_nested_types() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
live: null
collection:
    projections: null
    schema:
        $defs:
            MyTable:
                properties:
                    nested:
                        # Additional non-object type => selected.
                        type: [object, array]
model:
    require: null
"##,
        );
        insta::assert_debug_snapshot!(snap);
    }

    #[test]
    fn test_live_document_by_not_found() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
collection:
    projections:
        AltRoot: "" # Causes `flow_document` to be elided.
"##,
        );
        insta::assert_debug_snapshot!(snap.conflicts);
    }

    #[test]
    fn test_user_requires_connector_forbids() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
model:
    require:
        forbid: { whoop: sie }
"##,
        );
        insta::assert_debug_snapshot!(snap.conflicts);
    }

    #[test]
    fn test_user_excludes_connector_requires_location() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
validated:
    an_extra: { type: LOCATION_REQUIRED, reason: "i said so"}
"##,
        );
        insta::assert_debug_snapshot!(snap.conflicts);
    }

    #[test]
    fn test_user_excludes_connector_requires_field() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
validated:
    an_extra: { type: FIELD_REQUIRED, reason: "i said so"}
"##,
        );
        insta::assert_debug_snapshot!(snap.conflicts);
    }

    #[test]
    fn test_connector_requires_unknown_field() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
validated:
    sporks: { type: FIELD_REQUIRED, reason: "It's the fork of the future, man!"}
"##,
        );
        insta::assert_debug_snapshot!(snap.conflicts);
    }

    #[test]
    fn test_connector_requires_unknown_field_location() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
validated:
    sporks: { type: LOCATION_REQUIRED, reason: "It's also a spoon"}
"##,
        );
        insta::assert_debug_snapshot!(snap.conflicts);
    }

    #[test]
    fn test_connector_incompatible() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
collection:
    projections: null
validated:
    an_array: { type: INCOMPATIBLE, reason: "Wrong type in the DB and can't migrate"}
"##,
        );
        insta::assert_debug_snapshot!(snap); // Expect `an_array` is in selection.
    }

    #[test]
    fn test_no_objects_when_recommended_true() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
collection:
    projections: null
live: null
model:
    recommended: true
"##,
        );
        insta::assert_debug_snapshot!(snap.field_outcomes.get("a_map").unwrap(), @r###"
        Right(
            NotSelected,
        )
        "###);
    }

    #[test]
    fn test_excluded_canonical_parent() {
        let snap = run_test(
            include_str!("field_selection.fixture.yaml"),
            r##"
collection:
    projections: null
live: null
model:
    require: null
    recommended: 2 # Ordinarily `nested/*` is desired.
    exclude:
        - nested
"##,
        );
        // Verify that nested is excluded.
        insta::assert_debug_snapshot!(snap.field_outcomes.get("nested").unwrap(), @r###"
        Right(
            UserExcludes,
        )
        "###);
        // Verify that nested children are rejected due to excluded canonical parent.
        // These would normally be selected due to DesiredDepth at depth 2.
        insta::assert_debug_snapshot!(snap.field_outcomes.get("nested/bar").unwrap(), @r###"
        Right(
            ExcludedParent,
        )
        "###);
        insta::assert_debug_snapshot!(snap.field_outcomes.get("nested/baz").unwrap(), @r###"
        Right(
            ExcludedParent,
        )
        "###);
        insta::assert_debug_snapshot!(snap.field_outcomes.get("nested/foo").unwrap(), @r###"
        Right(
            ExcludedParent,
        )
        "###);
    }

    fn run_test(fixture_yaml: &str, patch_yaml: &str) -> Snap {
        let mut fixture: serde_json::Value = serde_yaml::from_str(fixture_yaml).unwrap();
        let patch: serde_json::Value = serde_yaml::from_str(patch_yaml).unwrap();
        () = json_patch::merge(&mut fixture, &patch);

        let Fixture {
            collection,
            model: model_fields,
            validated: validated_constraints,
            live: live_field_selection,
        }: Fixture = serde_json::from_value(fixture).unwrap();

        let scope = url::Url::parse("test://case").unwrap();
        let scope = crate::Scope::new(&scope);
        let mut errors = tables::Errors::new();

        let collection_projections = crate::collection::skim_projections(
            scope,
            &models::Collection::new("test/collection"),
            &collection,
            &mut errors,
        );
        assert_eq!(format!("{errors:?}"), "[]");

        let group_by = model_fields
            .group_by
            .iter()
            .map(|f| f.to_string())
            .collect::<Vec<_>>();

        let (selects, rejects, field_config) = extract_constraints(
            &collection_projections,
            &group_by,
            live_field_selection.as_ref(),
            &model_fields,
            &validated_constraints,
        );

        let snap_selects = selects
            .iter()
            .map(|(f, s)| (f.to_string(), s.clone()))
            .collect();
        let snap_rejects = rejects
            .iter()
            .map(|(f, r)| (f.to_string(), r.clone()))
            .collect();

        let (document_field, field_outcomes) = group_outcomes(
            &collection_projections,
            rejects,
            selects,
            &validated_constraints,
        );

        let (selection, conflicts) = build_selection(
            group_by.clone(),
            document_field.clone(),
            field_config,
            field_outcomes.clone(),
        );

        Snap {
            selects: snap_selects,
            rejects: snap_rejects,
            group_by,
            document: document_field,
            field_outcomes,
            selection,
            conflicts: conflicts.iter().map(|c| format!("{c:#}")).collect(),
        }
    }
}
