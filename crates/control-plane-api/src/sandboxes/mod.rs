//! A sandbox is a Linux machine where a user's AI agents run shell commands.
//! Each `internal.sandboxes` record has one machine for its whole life.
//! The record's `provider` column names the [`Provider`] which hosts it.
//! Commands run on the machine through `flowctl raw sandbox-exec`.

pub mod credentials;
pub mod docker;
pub mod exec;
#[cfg(test)]
pub mod fake;
pub mod provider;
pub mod sprites;

use anyhow::Context;
use provider::Provider;
use std::sync::Arc;

/// Prefix of every machine handle, which identifies the machines of sandboxes.
pub const HANDLE_PREFIX: &str = "sbx-";

/// Enables TCP keep-alive on `stream`. A sandbox command runs for as long as
/// its peers are connected, and an idle connection doesn't otherwise reveal a
/// peer which vanished without closing it. With Linux's default of 9 probes,
/// that's noticed within two minutes.
pub fn set_keepalive(stream: &tokio::net::TcpStream) -> std::io::Result<()> {
    let keepalive = socket2::TcpKeepalive::new()
        .with_time(std::time::Duration::from_secs(30))
        .with_interval(std::time::Duration::from_secs(10));
    socket2::SockRef::from(stream).set_tcp_keepalive(&keepalive)
}

#[derive(Debug, Clone)]
pub struct Sandbox {
    pub id: models::Id,
    pub user_id: uuid::Uuid,
    /// Names the machine to its provider. It derives from `id`, and so
    /// carries no user identifier.
    pub handle: String,
    /// Catalog name on which creation was authorized.
    pub catalog_name: String,
    pub created_at: chrono::DateTime<chrono::Utc>,
    /// [`Provider::kind`] of the provider which hosts the machine.
    pub provider: String,
    /// None until the machine is provisioned.
    pub provider_state: Option<provider::State>,
    /// When the provider reported that the machine no longer exists.
    pub gone_at: Option<chrono::DateTime<chrono::Utc>>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
pub enum State {
    /// The record reserves its catalog name while its machine is provisioned.
    Provisioning,
    Ready,
    /// The provider reported that the machine no longer exists. It's final:
    /// the record keeps its catalog name reserved until it's deleted.
    Gone,
}

impl Sandbox {
    pub fn state(&self) -> State {
        match (&self.provider_state, self.gone_at) {
            (_, Some(_)) => State::Gone,
            (None, None) => State::Provisioning,
            (Some(_), None) => State::Ready,
        }
    }
}

/// Arguments which configure the sandbox [`Providers`] of an agent.
#[derive(derivative::Derivative, clap::Args)]
#[derivative(Debug)]
pub struct Config {
    /// Kind of the sandbox provider which hosts new sandboxes: `sprites` or
    /// `docker`. That provider must also be configured. Without this flag,
    /// sandboxes can't be created, but configured providers still serve
    /// existing sandboxes.
    #[arg(long = "sandbox-provider", env = "SANDBOX_PROVIDER")]
    pub default_provider: Option<String>,
    /// Token for the Fly.io Sprites API. When provided, the Sprites sandbox
    /// provider is configured.
    #[derivative(Debug = "ignore")]
    #[arg(long = "sprites-token", env = "SPRITES_TOKEN", hide_env_values = true)]
    pub sprites_token: Option<String>,
    /// Download URL of the x86_64 Linux flowctl which Sprites sandboxes run.
    #[arg(
        long = "sprites-flowctl-url",
        env = "SPRITES_FLOWCTL_URL",
        default_value = "https://github.com/estuary/flow/releases/latest/download/flowctl-x86_64-linux"
    )]
    pub sprites_flowctl_url: url::Url,
    #[command(flatten)]
    pub docker: Option<docker::Docker>,
}

/// The providers which serve sandboxes, keyed by [`Provider::kind`].
/// Each record is served by the provider its `provider` column names, so
/// several providers can serve existing sandboxes side by side.
#[derive(Default)]
pub struct Providers {
    by_kind: std::collections::BTreeMap<&'static str, Arc<dyn Provider>>,
    /// Kind of the provider of new sandboxes. Without one, sandboxes can't
    /// be created.
    default_kind: Option<&'static str>,
}

impl Providers {
    pub fn new(
        providers: Vec<Arc<dyn Provider>>,
        default_kind: Option<&str>,
    ) -> anyhow::Result<Self> {
        let mut by_kind = std::collections::BTreeMap::new();
        for provider in providers {
            let kind = provider.kind();
            anyhow::ensure!(
                by_kind.insert(kind, provider).is_none(),
                "sandbox provider {kind:?} is configured more than once"
            );
        }
        let default_kind = default_kind
            .map(|kind| {
                by_kind
                    .get_key_value(kind)
                    .map(|(kind, _)| *kind)
                    .with_context(|| format!("default sandbox provider {kind:?} is not configured"))
            })
            .transpose()?;

        Ok(Self {
            by_kind,
            default_kind,
        })
    }

    pub fn from_config(config: Config) -> anyhow::Result<Self> {
        let Config {
            default_provider,
            sprites_token,
            sprites_flowctl_url,
            docker,
        } = config;
        let mut providers: Vec<Arc<dyn Provider>> = Vec::new();

        if let Some(token) = sprites_token {
            providers.push(Arc::new(sprites::Sprites {
                client: Arc::new(crate::sprites::Client::new(token)),
                flowctl_url: sprites_flowctl_url,
                prefix: HANDLE_PREFIX.to_string(),
            }));
        }
        if let Some(docker) = docker {
            providers.push(Arc::new(docker));
        }
        Self::new(providers, default_provider.as_deref())
    }

    /// The provider of new sandboxes.
    pub fn default_provider(&self) -> Option<&Arc<dyn Provider>> {
        self.default_kind.map(|kind| &self.by_kind[kind])
    }

    /// The provider which hosts the machine of `sandbox`.
    pub fn serving(&self, sandbox: &Sandbox) -> anyhow::Result<&Arc<dyn Provider>> {
        self.by_kind
            .get(sandbox.provider.as_str())
            .with_context(|| {
                format!(
                    "sandbox {} is hosted by provider {:?}, which is not configured",
                    sandbox.catalog_name, sandbox.provider
                )
            })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    #[error("Sandboxes are not configured")]
    NotConfigured,
    /// Catalog names are unique across all users.
    #[error("a sandbox named {0:?} already exists")]
    NameTaken(String),
    #[error("invalid catalog name: {0}")]
    InvalidName(String),
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// Inserts the record, which reserves `catalog_name`, and then provisions its
/// machine through the default provider. No database connection is held while
/// the machine is provisioned. Returns once the sandbox is ready.
///
/// If provisioning fails, the record is deleted. A provider which fails
/// part-way may leave a machine behind for the reaper.
///
/// If the record was deleted while its machine was provisioned (see [`delete`]),
/// or the provider state can't be stored, the machine is destroyed and the
/// record deleted.
pub async fn create(
    providers: &Providers,
    pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
    catalog_name: &str,
) -> Result<Sandbox, CreateError> {
    validator::Validate::validate(&models::Name::new(catalog_name))
        .map_err(|err| CreateError::InvalidName(err.to_string()))?;
    let provider = providers
        .default_provider()
        .ok_or(CreateError::NotConfigured)?;

    let mut sandbox = insert_record(pool, user_id, catalog_name, provider.kind()).await?;

    let state = match provider.provision(&sandbox.handle).await {
        Ok(state) => state,
        Err(err) => {
            let err = anyhow::Error::new(err).context("provisioning the sandbox machine");
            return Err(discard_record(pool, sandbox.id, err).await.into());
        }
    };

    if let Err(err) = store_provider_state(pool, sandbox.id, &state).await {
        let err = match provider.destroy(&sandbox.handle, &state).await {
            Ok(_) | Err(provider::Error::Gone) => err,
            Err(provider::Error::Other(destroy_err)) => anyhow::anyhow!(
                "{err:#}; destroying the provisioned machine also failed: {destroy_err:#}"
            ),
        };
        return Err(discard_record(pool, sandbox.id, err).await.into());
    }
    sandbox.provider_state = Some(state);

    tracing::info!(%sandbox.id, %sandbox.handle, %sandbox.provider, %user_id, "created sandbox");
    Ok(sandbox)
}

async fn insert_record(
    pool: &sqlx::PgPool,
    user_id: uuid::Uuid,
    catalog_name: &str,
    provider: &str,
) -> Result<Sandbox, CreateError> {
    // The handle derives from the generated id, so both come from one statement.
    // `replace` strips the colons of the macaddr8 text form.
    sqlx::query_as!(
        Sandbox,
        r#"
        insert into internal.sandboxes (id, user_id, handle, catalog_name, provider)
        select id, $1, $2 || replace(id::text, ':', ''), $3, $4
        from (select internal.id_generator() as id) as generated
        returning
            id as "id!: models::Id",
            user_id,
            handle,
            catalog_name,
            created_at,
            provider,
            provider_state,
            gone_at
        "#,
        user_id,
        HANDLE_PREFIX,
        catalog_name,
        provider,
    )
    .fetch_one(pool)
    .await
    .map_err(|err| match err.as_database_error() {
        Some(db) if db.constraint() == Some("sandboxes_catalog_name_idx") => {
            CreateError::NameTaken(catalog_name.to_string())
        }
        _ => CreateError::Other(anyhow::Error::new(err).context("creating sandbox record")),
    })
}

/// Fails if the record no longer exists.
async fn store_provider_state(
    pool: &sqlx::PgPool,
    id: models::Id,
    state: &provider::State,
) -> anyhow::Result<()> {
    let updated = sqlx::query!(
        r#"
        update internal.sandboxes
        set provider_state = $2, updated_at = now()
        where id = $1
        "#,
        id as models::Id,
        state,
    )
    .execute(pool)
    .await
    .context("storing the sandbox's provider state")?;

    anyhow::ensure!(
        updated.rows_affected() == 1,
        "the sandbox was deleted while its machine was provisioned"
    );
    Ok(())
}

/// Deletes the record of a failed creation, and returns `err`.
async fn discard_record(pool: &sqlx::PgPool, id: models::Id, err: anyhow::Error) -> anyhow::Error {
    let deleted = sqlx::query!(
        "delete from internal.sandboxes where id = $1",
        id as models::Id
    )
    .execute(pool)
    .await;

    match deleted {
        Ok(_) => err,
        Err(delete_err) => anyhow::Error::new(delete_err)
            .context(format!("removing failed sandbox creation after: {err:#}")),
    }
}

/// Fetches sandboxes whose catalog name is one of `names` or starts with one of
/// `prefixes`, newest first. The caller must have authorized access to each of
/// `names` and `prefixes`.
pub async fn fetch(
    pool: &sqlx::PgPool,
    names: &[String],
    prefixes: &[String],
) -> anyhow::Result<Vec<Sandbox>> {
    sqlx::query_as!(
        Sandbox,
        r#"
        select
            id as "id!: models::Id",
            user_id,
            handle,
            catalog_name,
            created_at,
            provider,
            provider_state,
            gone_at
        from internal.sandboxes
        where catalog_name = any($1) or catalog_name ^@ any($2)
        order by created_at desc
        "#,
        names,
        prefixes,
    )
    .fetch_all(pool)
    .await
    .context("fetching sandbox records")
}

/// Fetches the sandbox named `catalog_name`, if the caller of `claims` may use
/// it. A sandbox is usable only by the user who created it.
// TODO: Authorize by prefix, respecting token masks, once a sandbox capability
// exists for using sandboxes.
pub async fn fetch_owned(
    pool: &sqlx::PgPool,
    claims: &crate::ControlClaims,
    catalog_name: &str,
) -> anyhow::Result<Option<Sandbox>> {
    let sandboxes = fetch(pool, &[catalog_name.to_string()], &[]).await?;

    Ok(sandboxes
        .into_iter()
        .find(|sandbox| sandbox.user_id == claims.sub))
}

/// Records that the provider reported [`provider::Error::Gone`] for the
/// sandbox's machine.
pub async fn mark_gone(pool: &sqlx::PgPool, sandbox: &Sandbox) -> anyhow::Result<()> {
    sqlx::query!(
        r#"
        update internal.sandboxes
        set gone_at = now(), updated_at = now()
        where id = $1 and gone_at is null
        "#,
        sandbox.id as models::Id,
    )
    .execute(pool)
    .await
    .context("marking sandbox gone")?;

    tracing::info!(%sandbox.id, %sandbox.handle, %sandbox.provider, "sandbox machine is gone");
    Ok(())
}

/// Destroys the sandbox's machine through the provider which hosts it, and then
/// deletes its record. A machine which is already gone is not an error.
///
/// If destroying fails, the record is kept and the error returned. Until the
/// reaper exists, the record is the only reference to its machine, and keeping
/// it lets a retried delete destroy the machine.
///
/// A sandbox which is still provisioning has no machine to destroy yet, so only
/// its record is deleted. Its [`create`] then fails, and destroys the machine.
pub async fn delete(
    providers: &Providers,
    pool: &sqlx::PgPool,
    sandbox: &Sandbox,
) -> anyhow::Result<()> {
    if let Some(state) = &sandbox.provider_state {
        let provider = providers.serving(sandbox)?;

        match provider.destroy(&sandbox.handle, state).await {
            Ok(true) => {}
            Ok(false) | Err(provider::Error::Gone) => {
                tracing::info!(%sandbox.id, %sandbox.handle, "sandbox machine was already gone");
            }
            Err(provider::Error::Other(err)) => {
                return Err(err.context("destroying the sandbox machine"));
            }
        }
    }

    sqlx::query!(
        "delete from internal.sandboxes where id = $1",
        sandbox.id as models::Id,
    )
    .execute(pool)
    .await
    .context("deleting sandbox record")?;

    tracing::info!(%sandbox.id, %sandbox.handle, %sandbox.provider, "deleted sandbox");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::fake::Fake;
    use super::{Providers, Sandbox};
    use std::sync::Arc;

    const USER: uuid::Uuid = uuid::Uuid::from_bytes([0x11; 16]);

    async fn setup(pool: &sqlx::PgPool) {
        sqlx::query("insert into auth.users (id, email) values ($1, 'alice@example.test')")
            .bind(USER)
            .execute(pool)
            .await
            .unwrap();
    }

    fn providers(fakes: &[&Arc<Fake>], default_kind: Option<&str>) -> Providers {
        let fakes = fakes
            .iter()
            .map(|fake| Arc::clone(fake) as Arc<dyn super::Provider>)
            .collect();
        Providers::new(fakes, default_kind).unwrap()
    }

    /// Records keyed by catalog name, with handles replaced by catalog names
    /// so that snapshots are stable.
    async fn records(pool: &sqlx::PgPool) -> serde_json::Value {
        let sandboxes = super::fetch(pool, &[], &[String::new()]).await.unwrap();
        sandboxes
            .iter()
            .map(|sandbox| {
                assert!(sandbox.handle.starts_with(super::HANDLE_PREFIX));
                (
                    sandbox.catalog_name.clone(),
                    serde_json::json!({
                        "provider": sandbox.provider,
                        "provider_state": sandbox.provider_state,
                        "state": sandbox.state(),
                    }),
                )
            })
            .collect::<serde_json::Map<_, _>>()
            .into()
    }

    /// The fake's calls, with each handle replaced by the catalog name of its
    /// sandbox. `sandboxes` must include every sandbox the fake was called for.
    fn calls(fake: &Fake, sandboxes: &[&Sandbox]) -> Vec<String> {
        fake.calls()
            .into_iter()
            .map(|(op, handle)| {
                let sandbox = sandboxes
                    .iter()
                    .find(|sandbox| sandbox.handle == handle)
                    .expect("call is for a known sandbox");
                format!("{op} {}", sandbox.catalog_name)
            })
            .collect()
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn create_and_delete(pool: sqlx::PgPool) {
        setup(&pool).await;
        let fake = Arc::new(Fake::new("fake"));
        let providers = providers(&[&fake], Some("fake"));

        let sandbox = super::create(&providers, &pool, USER, "acmeCo/sandbox")
            .await
            .unwrap();
        let created = records(&pool).await;
        let machines = fake.machines();

        super::delete(&providers, &pool, &sandbox).await.unwrap();

        insta::assert_json_snapshot!(
            serde_json::json!({
                "returned": sandbox.state(),
                "created": created,
                "machinesAfterCreate": machines.len(),
                "calls": calls(&fake, &[&sandbox]),
                "afterDelete": records(&pool).await,
                "machinesAfterDelete": fake.machines().len(),
            }),
            @r#"
        {
          "afterDelete": {},
          "calls": [
            "provision acmeCo/sandbox",
            "destroy acmeCo/sandbox"
          ],
          "created": {
            "acmeCo/sandbox": {
              "provider": "fake",
              "provider_state": {
                "fake": "fake"
              },
              "state": "Ready"
            }
          },
          "machinesAfterCreate": 1,
          "machinesAfterDelete": 0,
          "returned": "Ready"
        }
        "#
        );
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn create_refusals(pool: sqlx::PgPool) {
        setup(&pool).await;
        let fake = Arc::new(Fake::new("fake"));
        let configured = providers(&[&fake], Some("fake"));
        let no_default = providers(&[&fake], None);

        let sandbox = super::create(&configured, &pool, USER, "acmeCo/sandbox")
            .await
            .unwrap();
        let outcomes = [
            super::create(&configured, &pool, USER, "acmeCo/sandbox").await,
            super::create(&configured, &pool, USER, "acmeCo/bad name").await,
            super::create(&no_default, &pool, USER, "acmeCo/other").await,
        ]
        .map(|outcome| outcome.unwrap_err().to_string());

        insta::assert_json_snapshot!(
            serde_json::json!({
                "outcomes": outcomes,
                "calls": calls(&fake, &[&sandbox]),
                "records": records(&pool).await,
            }),
            @r#"
        {
          "calls": [
            "provision acmeCo/sandbox"
          ],
          "outcomes": [
            "a sandbox named \"acmeCo/sandbox\" already exists",
            "invalid catalog name: : acmeCo/bad name doesn't match pattern [\\p{Letter}\\p{Number}\\-_\\.]+(/[\\p{Letter}\\p{Number}\\-_\\.]+)* (unmatched portion is:  name)",
            "Sandboxes are not configured"
          ],
          "records": {
            "acmeCo/sandbox": {
              "provider": "fake",
              "provider_state": {
                "fake": "fake"
              },
              "state": "Ready"
            }
          }
        }
        "#
        );
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn create_provision_failure(pool: sqlx::PgPool) {
        setup(&pool).await;
        let fake = Arc::new(Fake::new("fake"));
        let providers = providers(&[&fake], Some("fake"));

        fake.inner.lock().unwrap().fail_provision = true;
        let err = super::create(&providers, &pool, USER, "acmeCo/sandbox")
            .await
            .unwrap_err();
        let after_failure = records(&pool).await;

        // The failed creation released the name.
        fake.inner.lock().unwrap().fail_provision = false;
        super::create(&providers, &pool, USER, "acmeCo/sandbox")
            .await
            .unwrap();

        insta::assert_json_snapshot!(
            serde_json::json!({
                "error": format!("{err:#}"),
                "afterFailure": after_failure,
                "provisions": fake.calls().len(),
                "afterRetry": records(&pool).await,
            }),
            @r#"
        {
          "afterFailure": {},
          "afterRetry": {
            "acmeCo/sandbox": {
              "provider": "fake",
              "provider_state": {
                "fake": "fake"
              },
              "state": "Ready"
            }
          },
          "error": "provisioning the sandbox machine: fake provision failure",
          "provisions": 2
        }
        "#
        );
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn delete_while_provisioning(pool: sqlx::PgPool) {
        setup(&pool).await;
        let notify = Arc::new(tokio::sync::Notify::new());
        let fake = Arc::new(Fake {
            hold_provision: Some(notify.clone()),
            ..Fake::new("fake")
        });
        let providers = Arc::new(providers(&[&fake], Some("fake")));

        let create = tokio::spawn({
            let (providers, pool) = (providers.clone(), pool.clone());
            async move { super::create(&providers, &pool, USER, "acmeCo/sandbox").await }
        });
        while fake.calls().is_empty() {
            tokio::task::yield_now().await;
        }
        let provisioning = records(&pool).await;

        let sandbox = super::fetch(&pool, &["acmeCo/sandbox".to_string()], &[])
            .await
            .unwrap()
            .pop()
            .unwrap();
        super::delete(&providers, &pool, &sandbox).await.unwrap();

        notify.notify_one();
        let err = create.await.unwrap().unwrap_err();

        insta::assert_json_snapshot!(
            serde_json::json!({
                "provisioning": provisioning,
                "error": format!("{err:#}"),
                "calls": calls(&fake, &[&sandbox]),
                "machines": fake.machines().len(),
                "records": records(&pool).await,
            }),
            @r#"
        {
          "calls": [
            "provision acmeCo/sandbox",
            "destroy acmeCo/sandbox"
          ],
          "error": "the sandbox was deleted while its machine was provisioned",
          "machines": 0,
          "provisioning": {
            "acmeCo/sandbox": {
              "provider": "fake",
              "provider_state": null,
              "state": "Provisioning"
            }
          },
          "records": {}
        }
        "#
        );
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn delete_failures_and_gone_machines(pool: sqlx::PgPool) {
        setup(&pool).await;
        let fake = Arc::new(Fake::new("fake"));
        let providers = providers(&[&fake], Some("fake"));

        let mut created = Vec::new();
        for name in ["acmeCo/broken", "acmeCo/missing", "acmeCo/gone"] {
            created.push(super::create(&providers, &pool, USER, name).await.unwrap());
        }
        let [broken, missing, gone] = &created[..] else {
            unreachable!()
        };

        // The provider fails to destroy the machine.
        fake.inner.lock().unwrap().fail_destroy = true;
        let err = super::delete(&providers, &pool, broken).await.unwrap_err();
        fake.inner.lock().unwrap().fail_destroy = false;

        // The machine no longer exists, as `destroy` reports.
        fake.inner.lock().unwrap().machines.remove(&missing.handle);
        super::delete(&providers, &pool, missing).await.unwrap();

        // The provider reports the machine gone.
        super::mark_gone(&pool, gone).await.unwrap();
        let marked = records(&pool).await;
        fake.inner.lock().unwrap().gone = true;
        super::delete(&providers, &pool, gone).await.unwrap();

        insta::assert_json_snapshot!(
            serde_json::json!({
                "error": format!("{err:#}"),
                "marked": marked,
                "calls": calls(&fake, &[broken, missing, gone]),
                "records": records(&pool).await,
            }),
            @r#"
        {
          "calls": [
            "provision acmeCo/broken",
            "provision acmeCo/missing",
            "provision acmeCo/gone",
            "destroy acmeCo/broken",
            "destroy acmeCo/missing",
            "destroy acmeCo/gone"
          ],
          "error": "destroying the sandbox machine: fake destroy failure",
          "marked": {
            "acmeCo/broken": {
              "provider": "fake",
              "provider_state": {
                "fake": "fake"
              },
              "state": "Ready"
            },
            "acmeCo/gone": {
              "provider": "fake",
              "provider_state": {
                "fake": "fake"
              },
              "state": "Gone"
            }
          },
          "records": {
            "acmeCo/broken": {
              "provider": "fake",
              "provider_state": {
                "fake": "fake"
              },
              "state": "Ready"
            }
          }
        }
        "#
        );
    }

    #[sqlx::test(migrations = "../../supabase/migrations")]
    async fn providers_serve_their_own_records(pool: sqlx::PgPool) {
        setup(&pool).await;
        let alpha = Arc::new(Fake::new("alpha"));
        let beta = Arc::new(Fake::new("beta"));

        let first = super::create(
            &providers(&[&alpha, &beta], Some("alpha")),
            &pool,
            USER,
            "acmeCo/first",
        )
        .await
        .unwrap();
        let second = super::create(
            &providers(&[&alpha, &beta], Some("beta")),
            &pool,
            USER,
            "acmeCo/second",
        )
        .await
        .unwrap();
        let created = records(&pool).await;

        // A record whose provider isn't configured can't be deleted.
        let err = super::delete(&providers(&[&beta], Some("beta")), &pool, &first)
            .await
            .unwrap_err();

        let both = providers(&[&alpha, &beta], None);
        super::delete(&both, &pool, &second).await.unwrap();
        super::delete(&both, &pool, &first).await.unwrap();

        insta::assert_json_snapshot!(
            serde_json::json!({
                "created": created,
                "error": format!("{err:#}"),
                "alpha": calls(&alpha, &[&first, &second]),
                "beta": calls(&beta, &[&first, &second]),
                "records": records(&pool).await,
            }),
            @r#"
        {
          "alpha": [
            "provision acmeCo/first",
            "destroy acmeCo/first"
          ],
          "beta": [
            "provision acmeCo/second",
            "destroy acmeCo/second"
          ],
          "created": {
            "acmeCo/first": {
              "provider": "alpha",
              "provider_state": {
                "fake": "alpha"
              },
              "state": "Ready"
            },
            "acmeCo/second": {
              "provider": "beta",
              "provider_state": {
                "fake": "beta"
              },
              "state": "Ready"
            }
          },
          "error": "sandbox acmeCo/first is hosted by provider \"alpha\", which is not configured",
          "records": {}
        }
        "#
        );
    }

    #[test]
    fn config_arguments() {
        #[derive(clap::Parser)]
        struct Args {
            #[command(flatten)]
            sandboxes: super::Config,
        }
        let parse = |args: &[&str]| {
            let args = std::iter::once("agent").chain(args.iter().copied());
            // Ambient variables, such as a developer's SPRITES_TOKEN, are ignored.
            let command = <Args as clap::CommandFactory>::command();
            let matches = command
                .mut_args(|arg| arg.env(None::<&str>))
                .try_get_matches_from(args);
            match matches.and_then(|m| <Args as clap::FromArgMatches>::from_arg_matches(&m)) {
                Ok(Args { sandboxes }) => format!(
                    "default {:?}, sprites {}, docker {:?}",
                    sandboxes.default_provider,
                    sandboxes.sprites_token.is_some(),
                    sandboxes
                        .docker
                        .map(|docker| (docker.image, docker.mounts.len())),
                ),
                Err(err) => err.kind().to_string(),
            }
        };
        let docker = [
            "--docker-sandbox-image=img",
            "--docker-sandbox-flowctl=/bin/flowctl",
            "--docker-sandbox-scope=stack",
        ];

        insta::assert_json_snapshot!([
            parse(&[]),
            parse(&["--sandbox-provider=sprites", "--sprites-token=secret"]),
            parse(&docker),
            parse(&[&docker[..], &["--docker-sandbox-mount=/a:/b,/c:/d"]].concat()),
            parse(&docker[..2]),
            parse(&["--docker-sandbox-mount=/a:/b"]),
        ], @r#"
        [
          "default None, sprites false, docker None",
          "default Some(\"sprites\"), sprites true, docker None",
          "default None, sprites false, docker Some((\"img\", 0))",
          "default None, sprites false, docker Some((\"img\", 2))",
          "one or more required arguments were not provided",
          "one or more required arguments were not provided"
        ]
        "#);
    }

    #[test]
    fn providers_configuration() {
        let alpha: Arc<dyn super::Provider> = Arc::new(Fake::new("alpha"));
        let errors = [
            Providers::new(vec![alpha.clone(), alpha.clone()], None),
            Providers::new(vec![alpha], Some("beta")),
        ]
        .map(|result| result.err().unwrap().to_string());

        insta::assert_json_snapshot!(errors, @r#"
        [
          "sandbox provider \"alpha\" is configured more than once",
          "default sandbox provider \"beta\" is not configured"
        ]
        "#);
    }
}
