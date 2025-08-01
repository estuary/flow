mod alerts;
mod live_specs;

use async_graphql::{
    types::connection, Context, EmptyMutation, EmptySubscription, Object, Schema, SimpleObject,
};
use axum::Extension;
use chrono::{DateTime, Utc};
use live_specs::fetch_live_specs;
use models::Capability;
use models::{status::AlertType, CatalogType, Id};
use serde_json::value::RawValue;
use std::sync::Arc;

use crate::api::public::status::fetch_status;
use crate::api::{App, ControlClaims};

pub type GraphQLSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub struct QueryRoot;

#[Object]
impl QueryRoot {
    async fn captures(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Filter by catalog prefix")] prefixes: Vec<String>,
    ) -> async_graphql::Result<Vec<live_specs::LiveSpec>> {
        fetch_live_specs(ctx, models::CatalogType::Capture, prefixes).await
    }
    async fn collections(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Filter by catalog prefix")] prefixes: Vec<String>,
    ) -> async_graphql::Result<Vec<live_specs::LiveSpec>> {
        fetch_live_specs(ctx, models::CatalogType::Collection, prefixes).await
    }
    async fn materializations(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Filter by catalog prefix")] prefixes: Vec<String>,
    ) -> async_graphql::Result<Vec<live_specs::LiveSpec>> {
        fetch_live_specs(ctx, models::CatalogType::Materialization, prefixes).await
    }
    async fn tests(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Filter by catalog prefix")] prefixes: Vec<String>,
    ) -> async_graphql::Result<Vec<live_specs::LiveSpec>> {
        fetch_live_specs(ctx, models::CatalogType::Test, prefixes).await
    }

    /// Returns a list of alerts that are currently firing for the given catalog
    /// prefixes.
    async fn alerts(
        &self,
        ctx: &Context<'_>,
        #[graphql(desc = "Show alerts for the given catalog prefixes")] prefixes: Vec<String>,
    ) -> async_graphql::Result<Vec<alerts::Alert>> {
        alerts::list_alerts_firing(ctx, prefixes).await
    }

    /*
    async fn authorized_prefixes(
        &self,
        ctx: &Context<'_>,
        min_capability: Capability,
        after: Option<String>,
        first: Option<u32>,
    ) -> async_graphql::Result<Vec<String>> {
        let claims = ctx.data::<ControlClaims>().unwrap();
        let app = ctx.data::<App>().unwrap();
        async_graphql::types::connection::query(after, before, first, last, f)
        let prefixes = app.authorized_prefixes(claims).await?;
        Ok(prefixes)
    }
    */
}

pub fn create_schema() -> GraphQLSchema {
    Schema::build(QueryRoot, EmptyMutation, EmptySubscription).finish()
}

pub async fn graphql_handler(
    schema: Extension<GraphQLSchema>,
    claims: Extension<ControlClaims>,
    app_state: axum::extract::State<Arc<App>>,
    req: axum::extract::Json<async_graphql::Request>,
) -> axum::Json<async_graphql::Response> {
    let request = req.0.data(app_state.0).data(claims.0);

    let response = schema.execute(request).await;
    axum::Json(response)
}

/// Returns an HTML page for the GraphiQL interface, which allows users to
/// explore and interact with the GraphQL API. The html was copied from the
/// official example at:
/// https://github.com/graphql/graphiql/blob/0d9e51aa6452de1a1dee1ff1d1dae6df923f389f/examples/graphiql-cdn/index.html
/// The version of GraphiQL that's bundled with the `async_graphql` crate is out
/// of date, which is why we're using this html instead.
pub async fn graphql_graphiql() -> impl axum::response::IntoResponse {
    axum::response::Html(
        r#"
        <!doctype html>
        <html lang="en">
          <head>
            <meta charset="UTF-8" />
            <meta name="viewport" content="width=device-width, initial-scale=1.0" />
            <title>GraphiQL 5 with React 19 and GraphiQL Explorer</title>
            <style>
              body {
                margin: 0;
              }

              #graphiql {
                height: 100dvh;
              }

              .loading {
                height: 100%;
                display: flex;
                align-items: center;
                justify-content: center;
                font-size: 4rem;
              }
            </style>
            <link rel="stylesheet" href="https://esm.sh/graphiql/dist/style.css" />
            <link
              rel="stylesheet"
              href="https://esm.sh/@graphiql/plugin-explorer/dist/style.css"
            />
            <!--
             * Note:
             * The ?standalone flag bundles the module along with all of its `dependencies`, excluding `peerDependencies`, into a single JavaScript file.
             * `@emotion/is-prop-valid` is a shim to remove the console error ` module "@emotion /is-prop-valid" not found`. Upstream issue: https://github.com/motiondivision/motion/issues/3126
            -->
            <script type="importmap">
              {
                "imports": {
                  "react": "https://esm.sh/react@19.1.0",
                  "react/": "https://esm.sh/react@19.1.0/",

                  "react-dom": "https://esm.sh/react-dom@19.1.0",
                  "react-dom/": "https://esm.sh/react-dom@19.1.0/",

                  "graphiql": "https://esm.sh/graphiql?standalone&external=react,react-dom,@graphiql/react,graphql",
                  "graphiql/": "https://esm.sh/graphiql/",
                  "@graphiql/plugin-explorer": "https://esm.sh/@graphiql/plugin-explorer?standalone&external=react,@graphiql/react,graphql",
                  "@graphiql/react": "https://esm.sh/@graphiql/react?standalone&external=react,react-dom,graphql,@graphiql/toolkit,@emotion/is-prop-valid",

                  "@graphiql/toolkit": "https://esm.sh/@graphiql/toolkit?standalone&external=graphql",
                  "graphql": "https://esm.sh/graphql@16.11.0",
                  "@emotion/is-prop-valid": "data:text/javascript,"
                }
              }
            </script>
            <script type="module">
              import React from 'react';
              import ReactDOM from 'react-dom/client';
              import { GraphiQL, HISTORY_PLUGIN } from 'graphiql';
              import { createGraphiQLFetcher } from '@graphiql/toolkit';
              import { explorerPlugin } from '@graphiql/plugin-explorer';
              import 'graphiql/setup-workers/esm.sh';

              const fetcher = createGraphiQLFetcher({
                url: 'http://localhost:8675/api/v1/graphql',
              });
              const plugins = [HISTORY_PLUGIN, explorerPlugin()];

              function App() {
                return React.createElement(GraphiQL, {
                  fetcher,
                  plugins,
                  defaultEditorToolsVisibility: true,
                });
              }

              const container = document.getElementById('graphiql');
              const root = ReactDOM.createRoot(container);
              root.render(React.createElement(App));
            </script>
          </head>
          <body>
            <div id="graphiql">
              <div class="loading">Loading…</div>
            </div>
          </body>
        </html>
        "#,
    )
}
