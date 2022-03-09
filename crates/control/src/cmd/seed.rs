//! Seed some data into the development database. Useful for getting started
//! quickly after cloning or after a database reset.
use sqlx::PgPool;

use crate::cmd::{async_runtime, ConfigArgs};
use crate::config;
use crate::startup;

#[derive(clap::Args, Debug)]
pub struct Args {
    #[clap(flatten)]
    config: ConfigArgs,
}

pub fn run(args: Args) -> anyhow::Result<()> {
    config::load_settings(args.config.config_path)?;
    let runtime = async_runtime()?;

    runtime.block_on(async move {
        let db = startup::connect_to_postgres(&config::settings().database).await;
        seed_connectors(&db).await?;
        seed_accounts(&db).await?;

        Ok::<(), anyhow::Error>(())
    })?;

    Ok(())
}

async fn seed_accounts(db: &sqlx::Pool<sqlx::Postgres>) -> anyhow::Result<()> {
    use crate::models::accounts::NewAccount;
    use crate::repo::accounts::insert as insert_account;

    insert_account(
        db,
        NewAccount {
            display_name: "Administrator".to_owned(),
            email: "admin@localhost".to_owned(),
            name: "admin".to_owned(),
        },
    )
    .await?;

    Ok(())
}

async fn seed_connectors(db: &PgPool) -> anyhow::Result<()> {
    use crate::models::connector_images::NewConnectorImage;
    use crate::models::connectors::{ConnectorType, NewConnector};
    use crate::repo::connector_images::insert as insert_image;
    use crate::repo::connectors::insert as insert_connector;

    let hello_world = insert_connector(
        db,
        NewConnector {
            description: "A flood greetings.".to_owned(),
            name: "Hello World".to_owned(),
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await?;

    insert_image(
        db,
        NewConnectorImage {
            connector_id: hello_world.id,
            name: "ghcr.io/estuary/source-hello-world".to_owned(),
            digest: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
            tag: "01fb856".to_owned(),
        },
    )
    .await?;

    let source_postgres = insert_connector(
        db,
        NewConnector {
            description: "Read data from PostgreSQL.".to_owned(),
            name: "Postgres".to_owned(),
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await?;

    insert_image(
        db,
        NewConnectorImage {
            connector_id: source_postgres.id,
            name: "ghcr.io/estuary/source-postgres".to_owned(),
            digest: "88bd58892f66d105504e9ecc0ad921124decab22b60228359a2f72a4143ba529".to_owned(),
            tag: "f1bd86a".to_owned(),
        },
    )
    .await?;

    let materialize_postgres = insert_connector(
        db,
        NewConnector {
            description: "Write data to PostgreSQL.".to_owned(),
            name: "Postgres".to_owned(),
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Materialization,
        },
    )
    .await?;

    insert_image(
        db,
        NewConnectorImage {
            connector_id: materialize_postgres.id,
            name: "ghcr.io/estuary/materialize-postgres".to_owned(),
            digest: "fd2e8df5144eab45a54c6ab75d02284a73a199d0d3b8d200cab65bb811e1869d".to_owned(),
            tag: "898776b".to_owned(),
        },
    )
    .await?;

    Ok(())
}
