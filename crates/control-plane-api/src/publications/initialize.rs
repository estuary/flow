use anyhow::Context;
use itertools::Itertools;
use std::future::Future;

/// Initialize a draft prior to build/validation. This may add additional specs to the draft.
pub trait Initialize: Send + Sync {
    fn initialize(
        &self,
        db: &sqlx::PgPool,
        subject: &models::authz::Subject,
        draft: &mut tables::DraftCatalog,
    ) -> impl Future<Output = anyhow::Result<()>> + Send;
}

/// A no-op `Initialize` impl, for when you don't want to expand the draft.
pub struct NoopInitialize;
impl Initialize for NoopInitialize {
    async fn initialize(
        &self,
        _db: &sqlx::PgPool,
        _subject: &models::authz::Subject,
        _draft: &mut tables::DraftCatalog,
    ) -> anyhow::Result<()> {
        Ok(())
    }
}

impl<I1, I2> Initialize for (I1, I2)
where
    I1: Initialize,
    I2: Initialize,
{
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        subject: &models::authz::Subject,
        draft: &mut tables::DraftCatalog,
    ) -> anyhow::Result<()> {
        self.0.initialize(db, subject, draft).await?;
        self.1.initialize(db, subject, draft).await?;
        Ok(())
    }
}

/// An `Initialize` that expands the draft to touch live specs that read from or write to
/// any drafted collections. This may optionally filter the specs based on whether the user
/// holds `SpecEdit` to them.
pub struct ExpandDraft<'a> {
    /// Whether to filter specs based on the user's capability. If true, then only specs for which
    /// the user holds `SpecEdit` will be added to the draft.
    pub filter_user_can_edit: bool,
    /// Authorization Snapshot pinned for the publication, against which the
    /// user-capability filter is evaluated. Held as a field — rather than
    /// threaded through `Initialize::initialize` — because this is the only
    /// `Initialize` which evaluates authorization.
    pub snapshot: &'a crate::Snapshot,
}

impl Initialize for ExpandDraft<'_> {
    #[tracing::instrument(
        level = "debug",
        skip_all,
        err,
        fields(filter_user_can_edit = self.filter_user_can_edit)
    )]
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        subject: &models::authz::Subject,
        draft: &mut tables::DraftCatalog,
    ) -> anyhow::Result<()> {
        // Expand the set of drafted specs to include any tasks that read from or write to any of
        // the published collections. We do this so that validation can catch any inconsistencies
        // or failed tests that may be introduced by the publication.
        let drafted_collections = draft
            .collections
            .iter()
            .map(|d| d.collection.as_str())
            .collect::<Vec<_>>();
        let all_drafted_specs = draft.all_spec_names().collect::<Vec<_>>();

        let capability_filter = if self.filter_user_can_edit {
            Some(models::authz::Capability::SpecEdit)
        } else {
            None
        };
        let expanded_catalog = crate::live_specs::get_connected_live_specs(
            subject,
            &drafted_collections,
            &all_drafted_specs,
            capability_filter,
            db,
            self.snapshot,
        )
        .await?;
        tracing::debug!(
            expanded_names = %expanded_catalog.all_spec_names().format(","),
            "expanded draft"
        );

        draft.add_live(expanded_catalog);

        Ok(())
    }
}

pub struct RuntimeV2Rollout {
    /// When true, newly-created captures without an explicit flag run runtime v2.
    pub new_captures: bool,
    /// When true, newly-created materializations without an explicit flag run runtime v2.
    pub new_materializations: bool,
    /// When true, newly-created derivations without an explicit flag run runtime v2.
    pub new_derivations: bool,
}

impl Initialize for RuntimeV2Rollout {
    #[tracing::instrument(level = "debug", skip_all, err)]
    async fn initialize(
        &self,
        db: &sqlx::PgPool,
        _subject: &models::authz::Subject,
        draft: &mut tables::DraftCatalog,
    ) -> anyhow::Result<()> {
        let flag = models::Token::new(models::ENABLE_RUNTIME_V2);

        // A drafted task is a candidate for stamping when it's a real upsert
        // (not a touch or deletion) whose model hasn't set the flag explicitly.
        let is_candidate = |is_touch: bool, shards: &models::ShardTemplate| {
            !is_touch && !shards.flags.contains_key(&flag)
        };

        // Gather candidate names across the task types whose rollout is enabled.
        // Catalog names are globally unique, so all task types share the single
        // existence query below. Derivations are collections carrying a `derive`
        // block, and their shards live at `derive.shards` rather than `shards`.
        let mut candidates = Vec::new();
        if self.new_captures {
            candidates.extend(
                draft
                    .captures
                    .iter()
                    .filter(|row| {
                        row.model
                            .as_ref()
                            .is_some_and(|model| is_candidate(row.is_touch, &model.shards))
                    })
                    .map(|row| row.capture.to_string()),
            );
        }
        if self.new_materializations {
            candidates.extend(
                draft
                    .materializations
                    .iter()
                    .filter(|row| {
                        row.model
                            .as_ref()
                            .is_some_and(|model| is_candidate(row.is_touch, &model.shards))
                    })
                    .map(|row| row.materialization.to_string()),
            );
        }
        if self.new_derivations {
            candidates.extend(
                draft
                    .collections
                    .iter()
                    .filter(|row| {
                        row.model
                            .as_ref()
                            .and_then(|model| model.derive.as_ref())
                            .is_some_and(|derive| is_candidate(row.is_touch, &derive.shards))
                    })
                    .map(|row| row.collection.to_string()),
            );
        }
        if candidates.is_empty() {
            return Ok(());
        }

        // Only *new* tasks are enabled. A candidate that already has a
        // non-tombstone live spec is an update, so it's left as-is. A tombstone
        // (`spec is null`, a deleted spec a controller hasn't yet reaped) is
        // excluded by `spec is not null`, so re-creating a task counts as new.
        let existing: std::collections::HashSet<String> = sqlx::query!(
            r#"select catalog_name
               from live_specs
               where catalog_name = any($1::text[]) and spec is not null"#,
            &candidates as &[String],
        )
        .fetch_all(db)
        .await
        .context("fetching existing task names")?
        .into_iter()
        .map(|row| row.catalog_name)
        .collect();

        if self.new_captures {
            for row in draft.captures.iter_mut() {
                let Some(model) = row.model.as_mut() else {
                    continue;
                };
                if is_candidate(row.is_touch, &model.shards)
                    && !existing.contains(row.capture.as_str())
                {
                    model
                        .shards
                        .flags
                        .insert(flag.clone(), models::Token::new("true"));
                }
            }
        }
        if self.new_materializations {
            for row in draft.materializations.iter_mut() {
                let Some(model) = row.model.as_mut() else {
                    continue;
                };
                if is_candidate(row.is_touch, &model.shards)
                    && !existing.contains(row.materialization.as_str())
                {
                    model
                        .shards
                        .flags
                        .insert(flag.clone(), models::Token::new("true"));
                }
            }
        }
        if self.new_derivations {
            for row in draft.collections.iter_mut() {
                let Some(derive) = row.model.as_mut().and_then(|model| model.derive.as_mut())
                else {
                    continue;
                };
                if is_candidate(row.is_touch, &derive.shards)
                    && !existing.contains(row.collection.as_str())
                {
                    derive
                        .shards
                        .flags
                        .insert(flag.clone(), models::Token::new("true"));
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::publications::test_support::{
        alice, seed_alice_catalog, snapshot_with_grants, user_grant,
    };
    use models::authz::{CapabilityBundle, Subject};

    #[sqlx::test(
        migrations = "../../supabase/migrations",
        fixtures(path = "../fixtures", scripts("data_planes", "alice"))
    )]
    async fn test_expansion_filters_on_effective_bits(pool: sqlx::PgPool) {
        seed_alice_catalog(&pool).await;

        let legacy_admin = || {
            snapshot_with_grants(
                vec![user_grant(
                    alice(),
                    "aliceCo/",
                    models::Capability::Admin,
                    &[],
                )],
                Vec::new(),
            )
        };
        let editor_bundle_on_out = || {
            snapshot_with_grants(
                vec![user_grant(
                    alice(),
                    "aliceCo/out/",
                    models::Capability::None,
                    &[CapabilityBundle::Editor],
                )],
                Vec::new(),
            )
        };

        let unrestricted = Subject::unrestricted(alice());
        let viewer_mask = Subject {
            capability_mask: Some(CapabilityBundle::Viewer.capabilities()),
            ..unrestricted.clone()
        };
        let scoped_to_in = Subject {
            prefix_scope: Some("aliceCo/in/".to_string()),
            ..unrestricted.clone()
        };

        let cases: Vec<(&str, crate::Snapshot, &Subject, bool)> = vec![
            (
                "legacy admin, unrestricted",
                legacy_admin(),
                &unrestricted,
                true,
            ),
            (
                "legacy admin, viewer mask",
                legacy_admin(),
                &viewer_mask,
                true,
            ),
            (
                "legacy admin, scoped to aliceCo/in/",
                legacy_admin(),
                &scoped_to_in,
                true,
            ),
            (
                "legacy admin, viewer mask, unfiltered",
                legacy_admin(),
                &viewer_mask,
                false,
            ),
            (
                "editor bundle on aliceCo/out/ only",
                editor_bundle_on_out(),
                &unrestricted,
                true,
            ),
        ];

        let mut out = Vec::new();
        for (label, snapshot, subject, filter_user_can_edit) in cases {
            let mut draft = tables::DraftCatalog::default();
            draft.collections.insert(tables::DraftCollection {
                collection: models::Collection::new("aliceCo/data/foo"),
                scope: tables::synthetic_scope(models::CatalogType::Collection, "aliceCo/data/foo"),
                expect_pub_id: None,
                data_plane_id: models::Id::zero(),
                model: Some(models::CollectionDef::example()),
                is_touch: false,
            });

            ExpandDraft {
                filter_user_can_edit,
                snapshot: &snapshot,
            }
            .initialize(&pool, subject, &mut draft)
            .await
            .unwrap();

            let names = draft.all_spec_names().collect::<Vec<_>>();
            out.push(format!(
                "{label}: {names:?}, refresh requested {}",
                snapshot.revoke.is_cancelled()
            ));
        }
        insta::assert_snapshot!(out.join("\n"), @r#"
        legacy admin, unrestricted: ["aliceCo/in/capture-foo", "aliceCo/data/foo", "aliceCo/out/materialize-bar"], refresh requested false
        legacy admin, viewer mask: ["aliceCo/data/foo"], refresh requested false
        legacy admin, scoped to aliceCo/in/: ["aliceCo/in/capture-foo", "aliceCo/data/foo"], refresh requested false
        legacy admin, viewer mask, unfiltered: ["aliceCo/in/capture-foo", "aliceCo/data/foo", "aliceCo/out/materialize-bar"], refresh requested false
        editor bundle on aliceCo/out/ only: ["aliceCo/data/foo", "aliceCo/out/materialize-bar"], refresh requested false
        "#);
    }
}
