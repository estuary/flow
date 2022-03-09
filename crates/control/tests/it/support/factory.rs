use sqlx::PgPool;

use control::models::accounts::{Account, NewAccount};
use control::models::builds::Build;
use control::models::connector_images::{ConnectorImage, NewConnectorImage};
use control::models::connectors::{Connector, ConnectorType, NewConnector};
use control::models::id::Id;
use control::repo::accounts::insert as insert_account;
use control::repo::builds::insert as insert_build;
use control::repo::connector_images::insert as insert_image;
use control::repo::connectors::insert as insert_connector;

pub struct HelloWorldConnector;

impl HelloWorldConnector {
    pub fn attrs(&self) -> NewConnector {
        NewConnector {
            description: "A flood greetings.".to_owned(),
            name: "Hello World".to_owned(),
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        }
    }

    pub async fn create(&self, db: &PgPool) -> Connector {
        insert_connector(db, self.attrs())
            .await
            .expect("to insert test data")
    }
}

pub struct HelloWorldImage;

impl HelloWorldImage {
    pub fn attrs(&self, connector: &Connector) -> NewConnectorImage {
        NewConnectorImage {
            connector_id: connector.id,
            name: "ghcr.io/estuary/source-hello-world".to_owned(),
            digest: "15751ba960870e5ba233ebfe9663fe8a236c8ce213b43fbf4cccc4e485594600".to_owned(),
            tag: "01fb856".to_owned(),
        }
    }

    pub async fn create(&self, db: &PgPool, connector: &Connector) -> ConnectorImage {
        insert_image(&db, self.attrs(connector))
            .await
            .expect("to insert test data")
    }
}

pub struct KafkaConnector;

impl KafkaConnector {
    pub fn attrs(&self) -> NewConnector {
        NewConnector {
            description: "Reads from a Kafka topic".to_owned(),
            name: "Kafka".to_owned(),
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Source,
        }
    }

    pub async fn create(&self, db: &PgPool) -> Connector {
        insert_connector(db, self.attrs())
            .await
            .expect("to insert test data")
    }
}

pub struct KafkaImage;

impl KafkaImage {
    pub fn attrs(&self, connector: &Connector) -> NewConnectorImage {
        NewConnectorImage {
            connector_id: connector.id,
            name: "ghcr.io/estuary/source-kafka".to_owned(),
            digest: "34affba1ac24d67035309c64791e7c7b2f01fd26a934d91da16e262427b88a78".to_owned(),
            tag: "01fb856".to_owned(),
        }
    }

    pub async fn create(&self, db: &PgPool, connector: &Connector) -> ConnectorImage {
        insert_image(&db, self.attrs(connector))
            .await
            .expect("to insert test data")
    }
}

pub struct RocksetConnector;

impl RocksetConnector {
    pub fn attrs(&self) -> NewConnector {
        NewConnector {
            description: "Writes to Rockset".to_owned(),
            name: "Rockset".to_owned(),
            maintainer: "Estuary Technologies".to_owned(),
            r#type: ConnectorType::Materialization,
        }
    }

    pub async fn create(&self, db: &PgPool) -> Connector {
        insert_connector(db, self.attrs())
            .await
            .expect("to insert test data")
    }
}
pub struct RocksetImage;

impl RocksetImage {
    pub fn attrs(&self, connector: &Connector) -> NewConnectorImage {
        NewConnectorImage {
            connector_id: connector.id,
            name: "ghcr.io/estuary/materialize-rockset".to_owned(),
            digest: "8a1955c057ab52a769c7ed092c12c89678047132a05dcd0a8cc2f17c02411cc2".to_owned(),
            tag: "898776b".to_owned(),
        }
    }

    pub async fn create(&self, db: &PgPool, connector: &Connector) -> ConnectorImage {
        insert_image(&db, self.attrs(connector))
            .await
            .expect("to insert test data")
    }
}

pub struct AdminAccount;

impl AdminAccount {
    pub fn attrs(&self) -> NewAccount {
        NewAccount {
            display_name: "Admin".to_owned(),
            email: "admin@site.test".to_owned(),
            name: "admin".to_owned(),
        }
    }

    pub async fn create(&self, db: &PgPool) -> Account {
        insert_account(db, self.attrs())
            .await
            .expect("to insert test data")
    }
}

pub struct BatmanAccount;

impl BatmanAccount {
    pub fn attrs(&self) -> NewAccount {
        NewAccount {
            display_name: "Bruce Wayne".to_owned(),
            email: "batman@batcave.test".to_owned(),
            name: "batman".to_owned(),
        }
    }

    pub async fn create(&self, db: &PgPool) -> Account {
        insert_account(db, self.attrs())
            .await
            .expect("to insert test data")
    }
}

pub struct JokerAccount;

impl JokerAccount {
    pub fn attrs(&self) -> NewAccount {
        NewAccount {
            display_name: "Joker".to_owned(),
            email: "joker@batcave.test".to_owned(),
            name: "joker".to_owned(),
        }
    }

    pub async fn create(&self, db: &PgPool) -> Account {
        insert_account(db, self.attrs())
            .await
            .expect("to insert test data")
    }
}

pub struct AcmeBuild;

impl AcmeBuild {
    pub fn attrs(&self) -> serde_json::Value {
        serde_json::json!({
            "collections": {
                "acmeCo/collection": {
                    "key": ["/key"],
                    "schema": {
                        "type": "object",
                        "required": "key",
                        "properties": {
                            "key": {"type": "integer"}
                        }
                    }
                }
            }
        })
    }

    pub async fn create(&self, db: &PgPool, account_id: Id<Account>) -> Build {
        insert_build(db, self.attrs(), account_id)
            .await
            .expect("to insert test data")
    }
}
