mod db;

use crate::Snapshot;
pub use db::{Row, SpecRow, fetch_evolution, fetch_resource_spec_schema, resolve};
use itertools::Itertools;
pub use models::{Capability, evolutions::EvolvedCollection};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use std::{collections::BTreeSet, sync::Arc};

#[derive(Debug)]
pub struct Evolution {
    /// Draft into which the results of the evolution will be merged.
    pub draft: tables::DraftCatalog,
    /// Specifies which collections to evolve and how.
    pub requests: Vec<EvolveRequest>,
    /// The id of the user to act as. This is used to determine the permissions
    /// to specs, in case `require_user_can_admin` is `true`.
    pub user_id: uuid::Uuid,
    /// If `true`, then the evolution will not affect any captures or
    /// materializations that the user does not have `admin` capability to.
    /// Otherwise, user permissions will not limit which specs are affected.
    /// This should generally be set to `true` for user-initiated evolutions,
    /// and `false` for evolutions that are undertaken by our background
    /// automations.
    pub require_user_can_admin: bool,
    /// The instant the evolution was queued (the `evolutions` row's
    /// `updated_at`), which anchors authorization staleness: a Snapshot denial
    /// is authoritative only once the Snapshot postdates it. `None` — for
    /// callers without a durable queued instant — falls back to anchoring each
    /// denied spec on its own last publication time.
    pub started_at: Option<tokens::DateTime>,
}

#[derive(Debug)]
pub struct EvolutionOutput {
    /// The draft containing the results of the evolution.
    pub draft: tables::DraftCatalog,
    /// Summary of the actions that were taken, and which specs were affected.
    pub actions: Vec<EvolvedCollection>,
}

impl EvolutionOutput {
    pub fn is_success(&self) -> bool {
        self.draft.errors.is_empty()
    }
}

/// Rust struct corresponding to each array element of the `collections` JSON
/// input of an `evolutions` row.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
pub struct EvolveRequest {
    /// The current name of the collection.
    #[serde(alias = "old_name")]
    // alias can be removed after UI code is updated to use current_name
    pub current_name: String,
    /// Optional new name for the collection. If provided, the collection will be re-created.
    /// Otherwise, only materialization bindings will be updated.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub new_name: Option<String>,

    /// Whether to reset the collection. If `reset` is true, then `new_name`
    /// must _not_ be provided. When true, the collection will be reset, and
    /// will begin again with no data and no inferred schema.
    #[serde(default)]
    pub reset: bool,
    /// Optionally restrict updates to only the provided materializations. This conflicts with
    /// `new_name`, and at most one of the two may be provided.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub materializations: Vec<String>,
}

impl EvolveRequest {
    pub fn of(collection_name: impl Into<String>) -> EvolveRequest {
        EvolveRequest {
            current_name: collection_name.into(),
            new_name: None,
            materializations: Vec::new(),
            reset: false,
        }
    }

    pub fn reset(collection_name: impl Into<String>) -> EvolveRequest {
        EvolveRequest {
            current_name: collection_name.into(),
            reset: true,
            new_name: None,
            materializations: Vec::new(),
        }
    }

    pub fn with_new_name(mut self, new_name: impl Into<String>) -> Self {
        self.new_name = Some(new_name.into());
        self
    }

    pub fn with_version_increment(self) -> Self {
        let new_name = next_name(&self.current_name);
        self.with_new_name(new_name)
    }

    pub fn with_materializations(self, names: impl IntoIterator<Item = impl Into<String>>) -> Self {
        let materializations = names.into_iter().map(|n| n.into()).collect();
        Self {
            materializations,
            ..self
        }
    }

    fn validate(&self) -> anyhow::Result<()> {
        anyhow::ensure!(
            models::Collection::regex().is_match(&self.current_name),
            "current_name '{}' is invalid",
            self.current_name
        );
        if let Some(new_name) = &self.new_name {
            anyhow::ensure!(
                new_name != &self.current_name,
                "if new_name is provided, it must be different from current_name"
            );
            anyhow::ensure!(
                models::Collection::regex().is_match(new_name),
                "requested collection name '{new_name}' is invalid"
            );
            anyhow::ensure!(!self.reset, "reset must be false if new_name is provided");
        }
        Ok(())
    }
}

/// Fetches the specs needed by an evolutions job and applies user
/// authorization in-process against `snapshot`, replacing the recursive
/// `internal.user_roles()` filtering the fetch formerly did in SQL.
///
/// The user must hold `admin` to affect a live spec. A drafted spec whose
/// live counterpart is denied keeps its drafted side but loses the live join
/// (surfacing downstream as "was never published", the pre-existing
/// behavior); a denied not-drafted live spec is dropped entirely. As with
/// `live_specs::get_live_specs`, a denial is trusted only once the Snapshot
/// is authoritative for `started_at` — or, absent one, for the denied spec's
/// own last publication — and otherwise surfaces as a retryable
/// `AuthorizationSnapshotStale` error; cancelling the Snapshot's `revoke` to
/// request an early refresh is the caller's responsibility, as in `evolve`.
pub async fn resolve_specs(
    user_id: uuid::Uuid,
    draft_id: models::Id,
    collection_names: Vec<String>,
    txn: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    snapshot: &Snapshot,
    started_at: Option<tokens::DateTime>,
) -> anyhow::Result<Vec<SpecRow>> {
    let rows = db::fetch_evolution_specs(draft_id, collection_names, txn).await?;

    let mut out = Vec::with_capacity(rows.len());
    for mut row in rows {
        if let Some(last_pub_id) = row.last_pub_id {
            let authorized = snapshot.spec_fetch_authorization(
                user_id,
                &row.catalog_name,
                Capability::Admin,
                started_at,
                last_pub_id,
            )?;

            if !authorized {
                if row.draft_spec_id.is_none() {
                    continue;
                }
                row.live_spec_id = None;
                row.last_pub_id = None;
            }
        }
        out.push(row);
    }
    Ok(out)
}

#[tracing::instrument(skip_all, fields(user_id = %evolution.user_id))]
pub async fn evolve(
    evolution: Evolution,
    db: &PgPool,
    snapshot: Arc<dyn tokens::Watch<Snapshot>>,
) -> anyhow::Result<EvolutionOutput> {
    let Evolution {
        mut draft,
        requests,
        user_id,
        require_user_can_admin,
        started_at,
    } = evolution;
    for req in requests.iter() {
        if let Err(error) = req.validate() {
            let scope = tables::synthetic_scope(models::CatalogType::Collection, &req.current_name);
            draft.errors.insert(tables::Error {
                scope,
                error: error.context("validating evolution request"),
            });
        }
    }
    if !draft.errors.is_empty() {
        return Ok(EvolutionOutput {
            draft,
            actions: Vec::new(),
        });
    }

    // Fetch collections matching either the current or the new name. This
    // ensures that we can preserve the existing spec in case the `new_name`
    // names a collection that already exists.
    let mut fetch_collections = requests
        .iter()
        .flat_map(|r| std::iter::once(r.current_name.clone()).chain(r.new_name.clone().into_iter()))
        .collect::<BTreeSet<_>>();
    for r in draft.collections.iter() {
        fetch_collections.remove(r.collection.as_str());
    }
    let fetch_collections = fetch_collections.into_iter().collect::<Vec<_>>();
    let capability_filter = if require_user_can_admin {
        Some(Capability::Admin)
    } else {
        None
    };
    let snapshot = snapshot.token();
    let snapshot = snapshot.result().unwrap();
    let live_collections = match crate::live_specs::get_live_specs(
        user_id,
        &fetch_collections,
        capability_filter,
        db,
        snapshot,
        started_at,
    )
    .await
    {
        Ok(live) => live,
        Err(err) if validation::is_authz_snapshot_stale(&err) => {
            // A referenced collection was denied against a snapshot that
            // predates it. Request an early refresh so a retry sees the fresher
            // snapshot, and surface the retryable error.
            snapshot.revoke.cancel();
            return Err(err);
        }
        Err(err) => return Err(err),
    };

    draft.add_live(live_collections);

    let collection_names = requests
        .iter()
        .map(|r| r.current_name.as_str())
        .collect::<Vec<_>>();
    let exclude_names = draft.all_spec_names().collect::<Vec<_>>();
    let expanded_live = match crate::live_specs::get_connected_live_specs(
        user_id,
        &collection_names,
        &exclude_names,
        capability_filter,
        db,
        snapshot,
        started_at,
    )
    .await
    {
        Ok(live) => live,
        Err(err) if validation::is_authz_snapshot_stale(&err) => {
            snapshot.revoke.cancel();
            return Err(err);
        }
        Err(err) => return Err(err),
    };
    draft.add_live(expanded_live);

    let mut actions = Vec::new();
    for req in requests.iter() {
        match evolve_collection(&mut draft, req) {
            Ok(action) => {
                actions.push(action);
            }
            Err(error) => {
                let scope =
                    tables::synthetic_scope(models::CatalogType::Collection, &req.current_name);
                draft.errors.insert(tables::Error { scope, error });
            }
        }
    }
    Ok(EvolutionOutput { draft, actions })
}

#[tracing::instrument(err, skip_all, fields(current_name = %req.current_name, new_name = ?req.new_name))]
fn evolve_collection(
    draft: &mut tables::DraftCatalog,
    req: &EvolveRequest,
) -> anyhow::Result<EvolvedCollection> {
    let EvolveRequest {
        current_name,
        new_name,
        materializations,
        reset,
    } = req;

    // We only re-create collections if explicitly requested.
    let (re_create_collection, new_name) = match new_name.as_ref() {
        Some(n) => {
            anyhow::ensure!(!reset, "cannot reset collection if new name is provided");
            (true, n.to_owned())
        }
        None => (*reset, current_name.clone()),
    };
    let old_collection = models::Collection::new(current_name);
    let new_collection = models::Collection::new(new_name);

    // Add the new collection to the draft if needed. It's possible for the draft to already contain
    // a collection with this name, and we'll skip adding a new one in that case, in order to preserve
    // any changes that the user has potentially made in the draft.
    if re_create_collection && (*reset || draft.collections.get_by_key(&new_collection).is_none()) {
        anyhow::ensure!(
            materializations.is_empty(),
            "specific_materializations argument must be empty if collection is being re-created"
        );
        let Some(drafted) = draft.collections.get_by_key(&old_collection) else {
            anyhow::bail!("missing spec for collection '{current_name}'");
        };

        let Some(mut model) = drafted.model.clone() else {
            anyhow::bail!("draft catalog contained a deletion for collection '{current_name}'");
        };
        model.reset = *reset;

        let expect_pub_id = if *reset {
            drafted.expect_pub_id
        } else {
            Some(models::Id::zero())
        };
        let new_row = tables::DraftCollection {
            scope: drafted.scope.clone(),
            collection: new_collection.clone(),
            model: Some(model),
            expect_pub_id,
            is_touch: false,
        };
        draft.collections.upsert_overwrite(new_row);
    }

    // If re-creating the collection, remove the old one from the draft.
    if re_create_collection && !reset {
        let _ = draft.collections.remove_by_key(&old_collection);
    }

    let mut updated_materializations = Vec::new();

    for (materialization, draft_model, is_touch) in draft
        .materializations
        .iter_mut()
        .filter_map(|m| with_mat_binding(&old_collection, m))
    {
        if !materializations.is_empty()
            && !materializations
                .iter()
                .any(|m| m == materialization.as_str())
        {
            tracing::debug!(%materialization, "skipping materialization because it was not requested to be updated");
            continue;
        }

        *is_touch = false; // we're updating the materialization, so ensure it's not a touch.
        updated_materializations.push(materialization.to_string());
        for binding in draft_model
            .bindings
            .iter_mut()
            .filter(|b| b.source.collection() == &old_collection)
        {
            // If we're re-creating the collection, then update the source in place.
            // We do this even for disabled bindings, so that the spec is up to date
            // with the latest changes to the rest of the catalog.
            if re_create_collection {
                binding.source.set_collection(new_collection.clone());
            }

            // Don't update resources for disabled bindings.
            if binding.disable {
                tracing::debug!(%materialization, "skipping materialization because the binding is disabled");
                continue;
            }

            // Finally, we need to increment the backfill counter of the binding.
            // This is not _technically_ required for materializations when
            // re-creating the collection, since they'll backfill when the
            // collection name changes, anyway. But it may help to make it more
            // obvious and explicit, and certainly won't hurt anything.
            binding.backfill += 1;
        }
    }

    // If specific materializations were requested to be updated, ensure that
    // we were actually able to update all of the given materializations.
    if !materializations.is_empty() && materializations.len() != updated_materializations.len() {
        let actual = updated_materializations
            .iter()
            .map(|u| u.as_str())
            .collect::<BTreeSet<_>>();
        let diff = materializations
            .iter()
            .filter(|m| !actual.contains(m.as_str()))
            .format(", ");
        anyhow::bail!(
            "requested to update the materialization(s) [{diff}], but no such materializations were found that source from the collection '{old_collection}'"
        );
    }

    let mut updated_captures = Vec::new();
    // We don't need to update any captures if the collection isn't being re-created.
    if re_create_collection {
        for (capture, draft_model, is_touch) in draft
            .captures
            .iter_mut()
            .filter_map(|c| with_cap_binding(&old_collection, c))
        {
            updated_captures.push(capture.to_string());
            *is_touch = false; // we're updating the capture, so ensure it's not a touch.

            for binding in draft_model.bindings.iter_mut() {
                if &binding.target == &old_collection {
                    binding.target = new_collection.clone();
                    // When re-creating collections, it's quite likely that
                    // users will also want to trigger a new backfill. Unlike
                    // materializations, capture connectors will only backfill
                    // when the counter is incremented, not when only the
                    // collection name is changed.
                    binding.backfill += 1;
                }
            }
        }
    }

    // If we're re-creating the collection, then there's no requirement to have
    // updated any captures or materializations. But if we're _not_ re-creating
    // the collection and we still haven't updated any captures or
    // materializations, then consider this an error.
    if !re_create_collection && updated_captures.is_empty() && updated_materializations.is_empty() {
        anyhow::bail!("nothing to update for collection '{old_collection}'");
    }

    tracing::debug!(?updated_materializations, ?updated_captures, %re_create_collection, %new_collection, %old_collection, "evolved collection in draft");

    Ok(EvolvedCollection {
        old_name: old_collection.into(),
        new_name: new_collection.into(),
        updated_materializations,
        updated_captures,
    })
}

fn with_cap_binding<'a, 'b>(
    collection: &'a models::Collection,
    drafted: &'b mut tables::DraftCapture,
) -> Option<(
    &'b models::Capture,
    &'b mut models::CaptureDef,
    &'b mut bool,
)> {
    let tables::DraftCapture {
        capture,
        model,
        is_touch,
        ..
    } = drafted;
    let model = model.as_mut()?;
    if model.bindings.iter().any(|b| &b.target == collection) {
        Some((capture, model, is_touch))
    } else {
        None
    }
}

fn with_mat_binding<'a, 'b>(
    collection: &'a models::Collection,
    drafted: &'b mut tables::DraftMaterialization,
) -> Option<(
    &'b models::Materialization,
    &'b mut models::MaterializationDef,
    &'b mut bool,
)> {
    let tables::DraftMaterialization {
        materialization,
        model,
        is_touch,
        ..
    } = drafted;
    let model = model.as_mut()?;
    if model
        .bindings
        .iter()
        .any(|b| b.source.collection() == collection)
    {
        Some((materialization, model, is_touch))
    } else {
        None
    }
}

lazy_static::lazy_static! {
    static ref NAME_VERSION_RE: regex::Regex = regex::Regex::new(r#".*[_-][vV](\d+)$"#).unwrap();
}

/// These tests pin the privilege boundary of `resolve_specs` across its
/// migration from in-SQL `internal.user_roles()` filtering to Snapshot-based
/// authorization: nobody gains or loses authority relative to the legacy SQL,
/// except deltas inherent to the Rust grant walk
/// (`tables::UserGrant::is_authorized`), which is authoritative over the
/// legacy semantics. The test marked ACCEPTED DELTA pins such an intentional
/// difference; the rest are parity cases that held under both models.
#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{assert_stale_for, authoritative, published_at, stale};

    // From `fixtures/authz_specs.sql`. Carol is admin of `carolCo/`; Dan
    // administers only `danCo/` and so models an unauthorized caller.
    const CAROL: uuid::Uuid = uuid::uuid!("33333333-3333-3333-3333-333333333333");
    const DAN: uuid::Uuid = uuid::uuid!("44444444-4444-4444-4444-444444444444");
    const COLLECTION: &str = "carolCo/data/foo";
    const CAPTURE: &str = "carolCo/in/capture-foo";

    const DRAFT_ID: &str = "11:11:11:11:11:11:11:11";

    /// Inserts a draft owned by `user_id` which drafts each of `names`, and
    /// returns its id. Ids are fixed: tests run in isolated databases.
    async fn insert_draft(pool: &sqlx::PgPool, user_id: uuid::Uuid, names: &[&str]) -> models::Id {
        let draft_id: models::Id = sqlx::query_scalar(
            "insert into drafts (id, user_id) values ($1::flowid, $2) returning id",
        )
        .bind(DRAFT_ID)
        .bind(user_id)
        .fetch_one(pool)
        .await
        .expect("inserting draft");

        for (index, name) in names.iter().enumerate() {
            sqlx::query(
                r#"insert into draft_specs (id, draft_id, catalog_name, spec, spec_type)
                values ($1::flowid, $2::flowid, $3, '{}', 'collection')"#,
            )
            .bind(format!("22:22:22:22:22:22:22:{:02x}", index))
            .bind(DRAFT_ID)
            .bind(name)
            .execute(pool)
            .await
            .expect("inserting draft spec");
        }
        draft_id
    }

    /// Adds a user holding the given grants, mirroring shapes from
    /// `fixtures/attenuated_grants.sql`. `user_grant` is
    /// (object_role, capability, bundles-literal like `'{editor}'` or `'{}'`).
    async fn insert_user(
        pool: &sqlx::PgPool,
        user_id: uuid::Uuid,
        email: &str,
        user_grant: (&str, &str, &str),
        role_grants: &[(&str, &str, &str)],
    ) {
        sqlx::query("insert into auth.users (id, email) values ($1, $2)")
            .bind(user_id)
            .bind(email)
            .execute(pool)
            .await
            .expect("inserting user");

        let (object_role, capability, bundles) = user_grant;
        sqlx::query(
            r#"insert into user_grants (user_id, object_role, capability, bundles)
            values ($1, $2, $3::grant_capability, $4::capability_bundle[])"#,
        )
        .bind(user_id)
        .bind(object_role)
        .bind(capability)
        .bind(bundles)
        .execute(pool)
        .await
        .expect("inserting user grant");

        for (subject_role, object_role, capability) in role_grants {
            sqlx::query(
                r#"insert into role_grants (subject_role, object_role, capability)
                values ($1, $2, $3::grant_capability)"#,
            )
            .bind(subject_role)
            .bind(object_role)
            .bind(capability)
            .execute(pool)
            .await
            .expect("inserting role grant");
        }
    }

    /// Runs `resolve_specs` and returns rows sorted by catalog name.
    async fn resolve_sorted(
        pool: &sqlx::PgPool,
        user_id: uuid::Uuid,
        draft_id: models::Id,
        collection_names: &[&str],
        snapshot: &crate::Snapshot,
        started_at: Option<tokens::DateTime>,
    ) -> anyhow::Result<Vec<db::SpecRow>> {
        let mut txn = pool.begin().await.expect("begin");
        let mut rows = resolve_specs(
            user_id,
            draft_id,
            collection_names.iter().map(|n| n.to_string()).collect(),
            &mut txn,
            snapshot,
            started_at,
        )
        .await?;
        rows.sort_by(|l, r| l.catalog_name.cmp(&r.catalog_name));
        Ok(rows)
    }

    /// A user holding admin directly on `carolCo/` resolves both the drafted
    /// spec's live join and referenced not-drafted live specs.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_resolve_specs_admin_direct_grant(pool: sqlx::PgPool) {
        let draft_id = insert_draft(&pool, CAROL, &[COLLECTION]).await;
        // An authorized caller resolves identically under stale and
        // authoritative Snapshots: staleness only converts denials.
        for snapshot in [stale(&pool).await, authoritative(&pool).await] {
            let rows = resolve_sorted(&pool, CAROL, draft_id, &[CAPTURE], &snapshot, None)
                .await
                .expect("carol is admin of carolCo/");

            assert_eq!(2, rows.len(), "{rows:?}");
            assert_eq!(COLLECTION, rows[0].catalog_name);
            assert!(rows[0].draft_spec_id.is_some());
            assert!(rows[0].live_spec_id.is_some(), "live join populated");
            assert!(rows[0].last_pub_id.is_some());
            assert_eq!(CAPTURE, rows[1].catalog_name);
            assert!(rows[1].draft_spec_id.is_none());
            assert!(rows[1].live_spec_id.is_some());
        }
    }

    /// Admin reached through a transitive role grant (eve → eveCo/ → carolCo/)
    /// is equivalent to a direct grant, before and after the migration.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_resolve_specs_admin_via_transitive_role_grant(pool: sqlx::PgPool) {
        let eve = uuid::uuid!("77777777-7777-7777-7777-777777777777");
        insert_user(
            &pool,
            eve,
            "eve@example.com",
            ("eveCo/", "admin", "{}"),
            &[("eveCo/", "carolCo/", "admin")],
        )
        .await;

        let draft_id = insert_draft(&pool, eve, &[COLLECTION]).await;
        let rows = resolve_sorted(
            &pool,
            eve,
            draft_id,
            &[CAPTURE],
            &authoritative(&pool).await,
            None,
        )
        .await
        .expect("eve is admin of carolCo/ transitively");

        assert_eq!(2, rows.len(), "{rows:?}");
        assert!(rows[0].live_spec_id.is_some(), "live join populated");
        assert_eq!(CAPTURE, rows[1].catalog_name);
        assert!(rows[1].live_spec_id.is_some());
    }

    /// An unauthorized user's drafted spec loses its live join (surfacing
    /// later as "was never published"), and referenced not-drafted live specs
    /// are dropped entirely.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_resolve_specs_unauthorized_live_suppressed(pool: sqlx::PgPool) {
        let draft_id = insert_draft(&pool, DAN, &[COLLECTION]).await;
        let rows = resolve_sorted(
            &pool,
            DAN,
            draft_id,
            &[CAPTURE],
            &authoritative(&pool).await,
            None,
        )
        .await
        .expect("an authoritative denial is a silent suppression, not an error");

        assert_eq!(1, rows.len(), "capture must be dropped: {rows:?}");
        assert_eq!(COLLECTION, rows[0].catalog_name);
        assert!(rows[0].draft_spec_id.is_some());
        assert!(rows[0].live_spec_id.is_none(), "live join suppressed");
        assert!(rows[0].last_pub_id.is_none());
    }

    /// Drafted specs are always returned regardless of authorization; only
    /// their live joins are subject to it.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_resolve_specs_drafted_returned_regardless(pool: sqlx::PgPool) {
        let draft_id = insert_draft(&pool, DAN, &[COLLECTION, "danCo/new"]).await;
        let rows = resolve_sorted(&pool, DAN, draft_id, &[], &authoritative(&pool).await, None)
            .await
            .expect("drafted specs resolve regardless of authorization");

        assert_eq!(2, rows.len(), "{rows:?}");
        assert_eq!(COLLECTION, rows[0].catalog_name);
        assert!(rows[0].live_spec_id.is_none());
        assert_eq!("danCo/new", rows[1].catalog_name);
        assert!(rows[1].live_spec_id.is_none());
    }

    /// ACCEPTED DELTA — `internal.user_roles()` walked role_grants in a single
    /// direction (subject starts-with the held role), so admin held on
    /// `teamCo/nested/` could not use the `teamCo/ → carolCo/` grant and this
    /// shape was denied. The Rust walk (`tables::UserGrant::is_authorized`)
    /// also traverses grants whose subject is a *prefix* of the held role and
    /// authorizes it — an intentional widening; the Rust implementation is
    /// authoritative over the legacy SQL semantics (#control-plane,
    /// 2026-04-13).
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_resolve_specs_parent_subject_role_grant(pool: sqlx::PgPool) {
        let gwen = uuid::uuid!("88888888-8888-8888-8888-888888888888");
        insert_user(
            &pool,
            gwen,
            "gwen@example.com",
            ("teamCo/nested/", "admin", "{}"),
            &[("teamCo/", "carolCo/", "admin")],
        )
        .await;

        let draft_id = insert_draft(&pool, gwen, &[COLLECTION]).await;
        let rows = resolve_sorted(
            &pool,
            gwen,
            draft_id,
            &[CAPTURE],
            &authoritative(&pool).await,
            None,
        )
        .await
        .expect("gwen reaches carolCo/ through the parent-subject grant");

        assert_eq!(2, rows.len(), "{rows:?}");
        assert!(rows[0].live_spec_id.is_some(), "the Rust walk authorizes");
        assert_eq!(CAPTURE, rows[1].catalog_name);
        assert!(rows[1].live_spec_id.is_some());
    }

    /// PARITY — an attenuated path (raw `none` capability delegating only the
    /// `editor` bundle, then a raw-`admin` role grant) is denied under both
    /// models: `user_roles('admin')` rejects the first hop's capability, and
    /// the Rust walk attenuates the second hop's bits down to `editor`, which
    /// does not satisfy `Admin`.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_resolve_specs_attenuated_admin_denied(pool: sqlx::PgPool) {
        let erin = uuid::uuid!("55555555-5555-5555-5555-555555555555");
        insert_user(
            &pool,
            erin,
            "erin@example.com",
            ("sharedCo/", "none", "{editor}"),
            &[("sharedCo/", "carolCo/", "admin")],
        )
        .await;

        let draft_id = insert_draft(&pool, erin, &[COLLECTION]).await;
        let rows = resolve_sorted(
            &pool,
            erin,
            draft_id,
            &[CAPTURE],
            &authoritative(&pool).await,
            None,
        )
        .await
        .expect("an authoritative denial is a silent suppression, not an error");

        assert_eq!(1, rows.len(), "capture must be dropped: {rows:?}");
        assert!(rows[0].live_spec_id.is_none(), "live join suppressed");
    }

    /// A denial from a Snapshot which predates the denied spec's publication
    /// is provisional: it surfaces as a retryable `AuthorizationSnapshotStale`
    /// naming the spec, never as a silent suppression. Requesting an early
    /// refresh (`snapshot.revoke`) is the calling executor's job, as in
    /// `evolve`'s stale arms.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_resolve_specs_stale_denial_is_retryable(pool: sqlx::PgPool) {
        let draft_id = insert_draft(&pool, DAN, &[COLLECTION]).await;
        let err = resolve_sorted(&pool, DAN, draft_id, &[CAPTURE], &stale(&pool).await, None)
            .await
            .expect_err("a stale denial must be surfaced, not suppressed");
        assert_stale_for(err, COLLECTION);
    }

    /// `started_at` displaces the per-spec staleness anchor in both
    /// directions: a Snapshot which postdates the spec can still be stale for
    /// a later-queued evolution, and a Snapshot which predates the spec is
    /// authoritative for an earlier-queued one.
    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "authz_specs"))
    )]
    async fn test_resolve_specs_request_relative_staleness(pool: sqlx::PgPool) {
        let draft_id = insert_draft(&pool, DAN, &[COLLECTION]).await;
        let published = published_at(&pool).await;
        let skew = crate::Snapshot::TEMPORAL_SKEW;

        // Snapshot postdates the spec (authoritative per-spec) but predates
        // the queued evolution: the denial is provisional.
        let err = resolve_sorted(
            &pool,
            DAN,
            draft_id,
            &[CAPTURE],
            &authoritative(&pool).await,
            Some(published + skew * 8),
        )
        .await
        .expect_err("denial under a Snapshot older than the request is provisional");
        assert_stale_for(err, COLLECTION);

        // Snapshot predates the spec (stale per-spec) but postdates the queued
        // evolution: the denial is authoritative and silently suppresses.
        let rows = resolve_sorted(
            &pool,
            DAN,
            draft_id,
            &[CAPTURE],
            &stale(&pool).await,
            Some(published - skew * 8),
        )
        .await
        .expect("denial under a Snapshot newer than the request is authoritative");
        assert_eq!(1, rows.len(), "capture must be dropped: {rows:?}");
        assert!(rows[0].live_spec_id.is_none(), "live join suppressed");
    }
}

/// Takes an existing name and returns a new name with an incremeted version suffix.
/// The name `foo` will become `foo_v2`, and `foo_v2` will become `foo_v3` and so on.
fn next_name(current_name: &str) -> String {
    // Does the name already have a version suffix?
    // We try to work with whatever suffix is already present. This way, if a user
    // is starting with a collection like `acmeCo/foo-V3`, they'll end up with
    // `acmeCo/foo-V4` instead of `acmeCo/foo_v4`.
    if let Some(capture) = NAME_VERSION_RE.captures_iter(current_name).next() {
        if let Ok(current_version_num) = capture[1].parse::<u32>() {
            // wrapping_add is just to ensure we don't panic if someone passes
            // a naughty name with a u32::MAX version.
            return format!(
                "{}{}",
                current_name.strip_suffix(&capture[1]).unwrap(),
                // We don't really care what the collection name ends up as if
                // the old name is suffixed by "V-${u32::MAX}", as long as we don't panic.
                current_version_num.wrapping_add(1)
            );
        }
    }
    // We always use an underscore as the separator. This might look a bit
    // unseemly if dashes are already used as separators elsewhere in the
    // name, but any sort of heuristic for determining whether to use dashes
    // or underscores is rife with edge cases and doesn't seem worth the
    // complexity.
    format!("{current_name}_v2")
}
