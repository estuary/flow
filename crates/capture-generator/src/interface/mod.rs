//! The interface between source definition programs and the generator

use async_trait::async_trait;
use schemars::{schema::RootSchema, schema_for, JsonSchema};
use serde::Deserialize;

pub struct SourceDefinition<A: Auth, P: Pagination> {
    pub auth: A,
    pub pagination: P,
}

#[async_trait]
pub trait Auth {
    type Config: for<'de> Deserialize<'de> + JsonSchema;

    /// JSONSchema of configuration required for authentication
    /// e.g. api_key
    fn config_schema(&mut self) -> RootSchema {
        schema_for!(Self::Config)
    }

    /// Do any preparation necessary given the configuration
    /// You usually want to store the configuration at this stage
    fn prepare(&mut self, config: Self::Config) -> Result<(), anyhow::Error> {
        Ok(())
    }

    /// Given request, add necessary authentication to it
    async fn authenticate(&mut self, req: &mut reqwest::Request) -> Result<(), anyhow::Error>;

    /// Do any cleanup necessary. This is called when the connector is being shut down.
    fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[async_trait]
pub trait Pagination {
    type Config: for<'de> Deserialize<'de> + JsonSchema;

    /// JSONSchema of configuration required for pagination.
    /// e.g. page_size
    fn config_schema(&mut self) -> RootSchema {
        schema_for!(Self::Config)
    }

    /// Do any preparation necessary given the configuration
    fn prepare(&mut self, config: Self::Config) -> Result<(), anyhow::Error> {
        Ok(())
    }

    /// Given the configuration, paginate the API request given the last response from the API.
    /// It must return true if could find a way to navigate to the next page, otherwise returns false.
    async fn paginate(
        &mut self,
        req: &mut reqwest::Request,
        last_response: reqwest::Response,
    ) -> Result<bool, anyhow::Error>;

    /// Do any cleanup necessary. This is called when the connector is being shut down.
    fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

pub trait Stream {
    type Config: for<'de> Deserialize<'de> + JsonSchema;

    /// Key of the stream in configuration
    fn key(&mut self) -> String;

    /// Description of this stream
    fn description(&mut self) -> String;

    /// JSONSchema of configuration required for this stream
    fn config_schema(&mut self) -> RootSchema {
        schema_for!(Self::Config)
    }

    /// Do any preparation necessary given the configuration
    fn prepare(&mut self, config: Self::Config) -> Result<(), anyhow::Error> {
        Ok(())
    }

    /// Create a new request for this stream
    fn create_request(&mut self) -> reqwest::Request;

    /// The JSONSchema specification of the results from this stream
    fn spec(&mut self) -> RootSchema;

    /// Do any cleanup necessary. This is called when the connector is being shut down.
    fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
