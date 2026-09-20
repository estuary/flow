//! Resolve and evaluate the residency of tasks making data-plane requests.
//!
//! Resolution is deliberately neutral: a task is resident in its authenticated
//! issuing plane, resident elsewhere, or unknown to the Snapshot. Route policy
//! is layered on that state so that strict routes can deny non-residency today,
//! while redirecting routes can reuse resolution later.

/// The observed location of a catalog-named task relative to its authenticated
/// issuing data-plane.
pub(super) enum TaskLocation<'s> {
    Resident {
        task: &'s crate::snapshot::SnapshotTask,
        issuing_plane: &'s crate::snapshot::DataPlane,
    },
    Elsewhere {
        task: &'s crate::snapshot::SnapshotTask,
        issuing_plane: &'s crate::snapshot::DataPlane,
    },
    Unknown {
        issuing_plane: &'s crate::snapshot::DataPlane,
    },
}

/// How a strict residency policy treats a task absent from the Snapshot.
#[derive(Clone, Copy)]
pub(super) enum UnknownTaskPolicy {
    /// The operation requires an existing task.
    RequireKnown,
    /// An unpublished Discover or Validate task may establish residency through
    /// its longest covering storage mapping.
    AllowStorageMapping,
}

/// The successful outcome of strict task-residency evaluation.
pub(super) enum TaskResidency<'s> {
    Resident,
    StorageMappingRequired {
        data_plane: &'s crate::snapshot::DataPlane,
    },
}

/// Authenticate `token` and classify a catalog-named task relative to its
/// issuing data-plane, without deciding what a route does with that state.
pub(super) fn resolve_catalog_task_location<'s>(
    snapshot: &'s crate::Snapshot,
    task_name: &models::Name,
    data_plane_fqdn: &str,
    token: &str,
) -> tonic::Result<TaskLocation<'s>> {
    let Some(issuing_plane) = snapshot.verify_data_plane_token(data_plane_fqdn, token)? else {
        return Err(tonic::Status::unauthenticated(
            "no data-plane keys validated against the token signature",
        ));
    };

    Ok(match snapshot.task_by_catalog_name(task_name.as_str()) {
        Some(task) if task.data_plane_id == issuing_plane.control_id => TaskLocation::Resident {
            task,
            issuing_plane,
        },
        Some(task) => TaskLocation::Elsewhere {
            task,
            issuing_plane,
        },
        None => TaskLocation::Unknown { issuing_plane },
    })
}

/// Evaluate task residency under a strict policy.
///
/// A known task must reside in the authenticated issuing plane and have the
/// claimed type. An unknown task is either rejected or handed back for a
/// storage-mapping check. All successful outcomes carry the task's cordon so
/// [`crate::Envelope::authorization_outcome`] can settle migrations and stale
/// Snapshots consistently.
pub(super) fn evaluate_task_residency<'s>(
    snapshot: &'s crate::Snapshot,
    task_name: &models::Name,
    expected_type: models::CatalogType,
    data_plane_fqdn: &str,
    token: &str,
    unknown: UnknownTaskPolicy,
) -> crate::AuthZResult<TaskResidency<'s>> {
    let location = resolve_catalog_task_location(snapshot, task_name, data_plane_fqdn, token)?;

    let (issuing_plane, residency) = match location {
        TaskLocation::Resident {
            task,
            issuing_plane,
        } => {
            if task.spec_type != expected_type {
                return Err(tonic::Status::permission_denied(format!(
                    "task '{task_name}' is a {}, not a {expected_type}",
                    task.spec_type,
                )));
            }
            (issuing_plane, TaskResidency::Resident)
        }
        TaskLocation::Elsewhere {
            task,
            issuing_plane,
        } => {
            return Err(tonic::Status::permission_denied(format!(
                "task '{}' does not run in data-plane {}",
                task.task_name, issuing_plane.data_plane_fqdn,
            )));
        }
        TaskLocation::Unknown { issuing_plane } => match unknown {
            UnknownTaskPolicy::RequireKnown => {
                return Err(tonic::Status::failed_precondition(format!(
                    "task '{task_name}' is not known to the control-plane"
                )));
            }
            UnknownTaskPolicy::AllowStorageMapping => (
                issuing_plane,
                TaskResidency::StorageMappingRequired {
                    data_plane: issuing_plane,
                },
            ),
        },
    };

    Ok((
        snapshot.cordon_at(task_name.as_str(), issuing_plane),
        residency,
    ))
}

/// Settle the storage-mapping check that an unknown task defers to, mapping a
/// denial into the terminal status shared by every route which allows one.
///
/// Admissibility is settled before existence, so that a caller who fails it
/// doesn't learn from a 404 which secrets are there.
pub(super) async fn enforce_storage_mapping(
    pg_pool: &sqlx::PgPool,
    task_name: &models::Name,
    residency: TaskResidency<'_>,
) -> Result<(), crate::ApiError> {
    let TaskResidency::StorageMappingRequired { data_plane } = residency else {
        return Ok(());
    };
    if storage_mapping_admits(pg_pool, task_name, &data_plane.data_plane_name).await? {
        return Ok(());
    }

    Err(tonic::Status::permission_denied(format!(
        "task '{task_name}' is not known, and its storage mapping does not admit data-plane {}",
        data_plane.data_plane_name,
    ))
    .into())
}

/// Whether the longest storage mapping covering `task_name` admits
/// `data_plane_name`.
///
/// The longest mapping alone decides, mirroring publication's `lookup_mapping`;
/// a parent mapping's planes are never promoted into the decision. No covering
/// mapping at all, or one without the plane, denies.
async fn storage_mapping_admits(
    pg_pool: &sqlx::PgPool,
    task_name: &models::Name,
    data_plane_name: &str,
) -> sqlx::Result<bool> {
    // Storage mapping prefixes are always slash-terminated. Enumerating only
    // the covering candidates lets this query use the unique prefix index.
    let prefixes: Vec<&str> = task_name
        .as_str()
        .rmatch_indices('/')
        .map(|(index, _)| &task_name.as_str()[..index + 1])
        .collect();

    sqlx::query_scalar!(
        r#"
        SELECT COALESCE(
            (
                SELECT $2 IN (SELECT json_array_elements_text(m.spec -> 'data_planes'))
                FROM storage_mappings m
                WHERE m.catalog_prefix = ANY ($1::text[])
                ORDER BY length(m.catalog_prefix) DESC
                LIMIT 1
            ),
            false
        ) AS "admissible!: bool"
        "#,
        &prefixes as &[&str],
        data_plane_name,
    )
    .fetch_one(pg_pool)
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The fixture's two data-planes, as (FQDN, HMAC key). Cases name a plane
    /// as a pair so that a signature is never accidentally mismatched to its
    /// issuer -- `bad-signature` does so on purpose, and is the only one.
    const PLANE_ONE: (&str, &str) = ("fqdn1", "key1");
    const PLANE_TWO: (&str, &str) = ("fqdn2", "key3");

    /// Cases are aligned data; rustfmt's call-width budget would otherwise
    /// break each of them across six lines.
    #[rustfmt::skip]
    #[test]
    fn test_evaluate_task_residency() {
        use models::CatalogType::{Capture, Collection, Materialization};
        use UnknownTaskPolicy::{AllowStorageMapping, RequireKnown};

        let snapshot = crate::Snapshot::build_fixture(None);
        let cases = [
            ("resident", "acmeCo/source-pineapple", Capture, PLANE_ONE, RequireKnown),
            // A task of the other plane, at depth, whose cordon rides along
            // with the successful outcome.
            ("resident/nested", "bobCo/widgets/source-squash", Capture, PLANE_TWO, RequireKnown),
            ("elsewhere", "acmeCo/source-pineapple", Capture, PLANE_TWO, RequireKnown),

            ("type-mismatch", "acmeCo/source-pineapple", Materialization, PLANE_ONE, RequireKnown),
            // The mismatch is reported in catalog vocabulary, where a
            // derivation is a "collection" -- not in the label vocabulary of a
            // data-plane request, which calls the same thing a "derivation".
            ("type-mismatch/derivation", "acmeCo/source-pineapple", Collection, PLANE_ONE, RequireKnown),

            ("unknown/required", "acmeCo/source-new", Capture, PLANE_ONE, RequireKnown),
            ("unknown/storage-mapping", "acmeCo/source-new", Capture, PLANE_ONE, AllowStorageMapping),

            // A task migrating plane-one => plane-two is cordoned in its source
            // plane, and denied in its target until its residency actually moves.
            ("migration/source", "acmeCo/source-banana", Capture, PLANE_ONE, RequireKnown),
            ("migration/target", "acmeCo/source-banana", Capture, PLANE_TWO, RequireKnown),

            // An otherwise-valid request, signed with the other plane's key.
            ("bad-signature", "acmeCo/source-pineapple", Capture, (PLANE_ONE.0, PLANE_TWO.1), RequireKnown),
        ];

        let outcomes: Vec<_> = cases
            .into_iter()
            .map(|(label, task_name, task_type, (fqdn, hmac_key), unknown)| {
                let token = crate::test_server::data_plane_token(
                    fqdn,
                    hmac_key.as_bytes(),
                    proto_flow::capability::AUTHORIZE,
                    std::iter::empty::<(&str, &str)>(),
                    task_name,
                );
                let outcome = evaluate_task_residency(
                    &snapshot,
                    &models::Name::new(task_name),
                    task_type,
                    fqdn,
                    &token,
                    unknown,
                )
                .map(|(cordon_at, residency)| match residency {
                    TaskResidency::Resident => {
                        format!("resident cordoned={}", cordon_at.is_some())
                    }
                    TaskResidency::StorageMappingRequired { data_plane } => {
                        format!("storage-mapping {}", data_plane.data_plane_name)
                    }
                })
                .unwrap_or_else(|status| {
                    format!(
                        "{} {}",
                        tokens::rest::grpc_status_code_to_http(status.code()),
                        status.message()
                    )
                });
                (label, outcome)
            })
            .collect();

        insta::assert_debug_snapshot!(outcomes);
    }

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("storage_mappings"))
    )]
    async fn test_storage_mapping_admission(pool: sqlx::PgPool) {
        const PLANE_ONE: &str = "ops/dp/public/aws-us-west-2-c1";
        const PLANE_TWO: &str = "ops/dp/public/gcp-us-central1-c2";
        let cases = [
            ("parent/admitted", "aliceCo/capture-new", PLANE_ONE),
            ("parent/other-plane", "aliceCo/capture-new", PLANE_TWO),
            (
                "nested/parent-not-promoted",
                "aliceCo/private/capture-new",
                PLANE_ONE,
            ),
            ("nested/admitted", "aliceCo/private/capture-new", PLANE_TWO),
            ("no-mapping", "carolCo/capture-new", PLANE_ONE),
        ];

        let mut outcomes = Vec::new();
        for (label, task_name, data_plane_name) in cases {
            outcomes.push((
                label,
                storage_mapping_admits(&pool, &models::Name::new(task_name), data_plane_name)
                    .await
                    .unwrap(),
            ));
        }

        insta::assert_debug_snapshot!(outcomes, @r###"
        [
            (
                "parent/admitted",
                true,
            ),
            (
                "parent/other-plane",
                false,
            ),
            (
                "nested/parent-not-promoted",
                false,
            ),
            (
                "nested/admitted",
                true,
            ),
            (
                "no-mapping",
                false,
            ),
        ]
        "###);
    }
}
