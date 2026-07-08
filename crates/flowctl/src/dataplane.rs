//! flowctl-local data-plane authorization helpers.
//!
//! These wrap the `flow_client_next::workflows` authorization sources with the
//! CliContext's shared, live user-token watch (`tokens::watch`). The returned
//! journal/shard clients hold the spawned authorization watch alive, which in
//! turn holds a clone of the user-token watch -- so a long-lived read keeps
//! re-minting its data-plane tokens using a currently-valid user access token
//! and survives rotation of both token layers.

use flow_client_next::user_auth::UserToken;
use flow_client_next::workflows;

/// Authorize the user to read a collection's journals, returning the journal
/// name prefix and a journal client.
pub async fn user_collection_journal(
    rest: &flow_client_next::rest::Client,
    user_tokens: &tokens::PendingWatch<UserToken>,
    router: &gazette::Router,
    collection: &str,
    capability: models::Capability,
) -> anyhow::Result<(String, gazette::journal::Client)> {
    let watch = tokens::watch(workflows::UserCollectionAuth {
        client: rest.clone(),
        user_tokens: user_tokens.clone(),
        collection: models::Collection::new(collection),
        capability,
    });

    let journal_name_prefix = watch
        .ready()
        .await
        .token()
        .result()?
        .journal_name_prefix
        .clone();

    let journal_client = workflows::user_collection_auth::new_journal_client(
        gazette::journal::Client::new_fragment_client(),
        router.clone(),
        watch,
    );

    Ok((journal_name_prefix, journal_client))
}

/// Authorize the user to access a task, returning its shard id prefix, ops
/// journal names, and shard + journal clients.
pub async fn user_task_authorization(
    rest: &flow_client_next::rest::Client,
    user_tokens: &tokens::PendingWatch<UserToken>,
    router: &gazette::Router,
    task: &str,
) -> anyhow::Result<(
    String,
    String,
    String,
    gazette::shard::Client,
    gazette::journal::Client,
)> {
    let watch = tokens::watch(workflows::UserTaskAuth {
        client: rest.clone(),
        user_tokens: user_tokens.clone(),
        task: models::Name::new(task),
        capability: models::Capability::Read,
    });

    let (shard_id_prefix, ops_logs_journal, ops_stats_journal) = {
        let ready = watch.ready().await.token();
        let model = ready.result()?;
        (
            model.shard_id_prefix.clone(),
            model.ops_logs_journal.clone(),
            model.ops_stats_journal.clone(),
        )
    };

    let journal_client = workflows::user_task_auth::new_journal_client(
        gazette::journal::Client::new_fragment_client(),
        router.clone(),
        watch.clone(),
    );
    let shard_client = workflows::user_task_auth::new_shard_client(router.clone(), watch);

    Ok((
        shard_id_prefix,
        ops_logs_journal,
        ops_stats_journal,
        shard_client,
        journal_client,
    ))
}

/// Authorize the user for administrative operations over a task's shards
/// and recovery logs, returning the task's ops journal names and
/// Admin-capability shard + journal clients.
pub async fn user_task_admin(
    rest: &flow_client_next::rest::Client,
    user_tokens: &tokens::PendingWatch<UserToken>,
    router: &gazette::Router,
    task: &str,
    data_plane: models::Name,
) -> anyhow::Result<(
    String,
    String,
    gazette::shard::Client,
    gazette::journal::Client,
)> {
    let (ops_logs_journal, ops_stats_journal) = {
        let watch = tokens::watch(workflows::UserTaskAuth {
            client: rest.clone(),
            user_tokens: user_tokens.clone(),
            task: models::Name::new(task),
            capability: models::Capability::Read,
        });
        let ready = watch.ready().await.token();
        let model = ready.result()?;

        (
            model.ops_logs_journal.clone(),
            model.ops_stats_journal.clone(),
        )
    };

    let watch = tokens::watch(workflows::UserPrefixAuth {
        client: rest.clone(),
        user_tokens: user_tokens.clone(),
        prefix: models::Prefix::new(format!("{task}/")),
        data_plane,
        capability: models::Capability::Admin,
    });
    let journal_client = workflows::user_prefix_auth::new_journal_client(
        gazette::journal::Client::new_fragment_client(),
        router.clone(),
        watch.clone(),
    );
    let shard_client = workflows::user_prefix_auth::new_shard_client(router.clone(), watch);

    Ok((
        ops_logs_journal,
        ops_stats_journal,
        shard_client,
        journal_client,
    ))
}

/// Authorize the user to access a catalog prefix within a data-plane,
/// returning the authorization model (broker/reactor addresses and tokens).
pub async fn user_prefix_authorization(
    rest: &flow_client_next::rest::Client,
    user_tokens: &tokens::PendingWatch<UserToken>,
    prefix: models::Prefix,
    data_plane: models::Name,
    capability: models::Capability,
) -> anyhow::Result<models::authorizations::UserPrefixAuthorization> {
    let watch = tokens::watch(workflows::UserPrefixAuth {
        client: rest.clone(),
        user_tokens: user_tokens.clone(),
        prefix,
        data_plane,
        capability,
    });

    // UserPrefixAuthorization is not Clone, so rebuild it from the watched model.
    let refresh = watch.ready().await.token();
    let models::authorizations::UserPrefixAuthorization {
        broker_address,
        broker_token,
        reactor_address,
        reactor_token,
        retry_millis,
    } = refresh.result()?;

    Ok(models::authorizations::UserPrefixAuthorization {
        broker_address: broker_address.clone(),
        broker_token: broker_token.clone(),
        reactor_address: reactor_address.clone(),
        reactor_token: reactor_token.clone(),
        retry_millis: *retry_millis,
    })
}
