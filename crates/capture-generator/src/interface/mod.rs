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
    fn prepare(&mut self, _: Self::Config) -> Result<(), anyhow::Error> {
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
    fn prepare(&mut self, _: Self::Config) -> Result<(), anyhow::Error> {
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

#[async_trait]
pub trait RateLimiter {
    type Config: for<'de> Deserialize<'de> + JsonSchema;

    /// JSONSchema of configuration required for pagination.
    /// e.g. page_size
    fn config_schema(&mut self) -> RootSchema {
        schema_for!(Self::Config)
    }

    /// Do any preparation necessary given the configuration
    fn prepare(&mut self, _: Self::Config) -> Result<(), anyhow::Error> {
        Ok(())
    }

    /// Ratelimit a new request before it is sent, based on the last response received from the stream
    /// This will run after pagination, and as a result may tamper with the values that the pagination has set
    // TODO: Can we somehow make this smart so that it works in conjunction with paginate? e.g. if paginate
    // is setting page_size and ratelimit wants to change that, how do we handle that? Ideally they also use the same configuration
    // should they be the same interface?
    async fn ratelimit_request(
        &mut self,
        req: &mut reqwest::Request,
        last_response: reqwest::Response,
    ) -> Result<(), anyhow::Error>;

    /// Given the last response received, handle rate_limiting between requests (this can be a thread::sleep if waiting is necessary)
    // TODO: do we need to provide more statistics to ratelimiters so they can be more smart?
    async fn ratelimit_between_requests(
        &mut self,
        last_response: reqwest::Response,
    ) -> Result<(), anyhow::Error>;

    /// Do any cleanup necessary. This is called when the connector is being shut down.
    fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}

#[async_trait]
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

    /// The JSONSchema specification of the results from this stream
    async fn spec(&mut self) -> Result<RootSchema, anyhow::Error>;

    /// Do any preparation necessary given the configuration
    fn prepare(&mut self, _: Self::Config) -> Result<(), anyhow::Error> {
        Ok(())
    }

    /// Create a new request for this stream
    fn create_request(&mut self) -> reqwest::Request;

    /// Do any cleanup necessary. This is called when the connector is being shut down.
    fn cleanup(&mut self) -> Result<(), anyhow::Error> {
        Ok(())
    }
}
