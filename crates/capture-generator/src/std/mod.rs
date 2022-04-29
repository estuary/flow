use async_trait::async_trait;
use http::header::AUTHORIZATION;
use reqwest::{Request, Url};
use schemars::{schema::RootSchema, JsonSchema};
use serde::Deserialize;

use crate::interface::*;

#[derive(Default)]
pub struct BearerTokenAuth {
    config: Option<BearerTokenConfig>,
}
#[derive(Deserialize, JsonSchema)]
pub struct BearerTokenConfig {
    token: String,
}
#[async_trait]
impl Auth for BearerTokenAuth {
    type Config = BearerTokenConfig;

    fn prepare(&mut self, config: BearerTokenConfig) -> Result<(), anyhow::Error> {
        self.config = Some(config);
        Ok(())
    }

    async fn authenticate(&mut self, req: &mut Request) -> Result<(), anyhow::Error> {
        let token = &self.config.as_ref().unwrap().token;
        let headers = req.headers_mut();
        headers.insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
        Ok(())
    }
}

/// A very basic stream that sends a HTTP request to a specific URL.
/// Can be used alongside different paginations to create a stream.
pub struct BasicStream {
    pub key: String,
    pub description: String,
    pub endpoint: String,
    pub method: http::Method,
    pub spec: RootSchema,
}
impl Stream for BasicStream {
    type Config = ();

    fn key(&mut self) -> String {
        self.key.clone()
    }

    fn description(&mut self) -> String {
        self.description.clone()
    }

    fn create_request(&mut self) -> Request {
        let mut request = Request::new(self.method.clone(), Url::parse(&self.endpoint).unwrap());
        request
            .headers_mut()
            .insert("User-Agent", "flow-capture".parse().unwrap());

        let body = request.body_mut();
        *body = Some(vec![].into());

        request
    }

    fn spec(&mut self) -> RootSchema {
        self.spec.clone()
    }
}

pub struct BasicPagination {
    pub page_query_field: String,
    pub current_page_field: String,
    pub max_pages_field: String,
}

#[async_trait]
impl Pagination for BasicPagination {
    type Config = ();

    async fn paginate(
        &mut self,
        req: &mut reqwest::Request,
        last_response: reqwest::Response,
    ) -> Result<bool, anyhow::Error> {
        let last_body = last_response.text().await?;
        println!("last_body {:#?}", last_body);
        let last_body = serde_json::from_str::<serde_json::Value>(&last_body)?;

        let res_current_page = last_body.get(&self.current_page_field).unwrap();
        let res_max_page = last_body.get(&self.max_pages_field).unwrap();

        if res_current_page == res_max_page {
            Ok(false)
        } else {
            let current_page = res_current_page.as_u64().unwrap();
            let next_page = (current_page + 1).to_string();

            req.url_mut()
                .query_pairs_mut()
                .append_pair(&self.page_query_field, &next_page);

            println!("{:#?}", req.url().query());

            Ok(true)
        }
    }
}
