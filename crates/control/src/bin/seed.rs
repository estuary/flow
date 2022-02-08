//! Seed some data into the development database. Useful for getting started
//! quickly after cloning or after a database reset.

use sqlx::PgPool;

use control::config;
use control::models::connector_images::CreateConnectorImage;
use control::models::connectors::{ConnectorType, CreateConnector};
use control::repo::connector_images::insert;
use control::repo::connectors::insert as insert_connector;
use control::startup;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let settings = config::settings();
    let db = startup::connect_to_postgres(&settings.database).await;

    seed_connectors(&db).await.expect("to seed connectors");
}

async fn seed_connectors(db: &PgPool) -> Result<(), anyhow::Error> {
    let hello_world = insert_connector(
        db,
        CreateConnector {
            description: "A flood greetings.".to_owned(),
            name: "Hello World".to_owned(),
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await?;

    insert(
        db,
        CreateConnectorImage {
            connector_id: hello_world.id,
            name: "ghcr.io/estuary/source-hello-world".to_owned(),
            digest: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
            tag: "01fb856".to_owned(),
        },
    )
    .await?;

    let postgres = insert_connector(
        db,
        CreateConnector {
            description: "Read data from PostgreSQL.".to_owned(),
            name: "Postgres".to_owned(),
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        },
    )
    .await?;

    insert(
        db,
        CreateConnectorImage {
            connector_id: postgres.id,
            name: "ghcr.io/estuary/source-postgres".to_owned(),
            digest: "88bd58892f66d105504e9ecc0ad921124decab22b60228359a2f72a4143ba529".to_owned(),
            tag: "f1bd86a".to_owned(),
        },
    )
    .await?;

    Ok(())
}
