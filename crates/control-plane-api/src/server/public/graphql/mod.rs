//! GraphQL API
//!
//! The `QueryRoot`
mod alerts;
pub mod id;
mod live_spec_refs;
mod live_specs;
mod prefixes;
mod publication_history;
pub mod status;

use async_graphql::{EmptyMutation, EmptySubscription, Schema};
use axum::Extension;
use std::sync::Arc;

use crate::server::{App, ControlClaims};

// This type represents the complete graphql schema.
pub type GraphQLSchema = Schema<QueryRoot, EmptyMutation, EmptySubscription>;

pub struct PgDataLoader(pub sqlx::PgPool);

// Represents the portion of the GraphQL schema that deals with read-only
// queries. This is a composition of the queries from various modules here. Note
// that the repetition in those names is intentional, because async-graphql does
// not accept, for example `live_specs::Query` and `prefixes::Query`. Each query
// struct must have a unique name.
#[derive(Debug, Default, async_graphql::MergedObject)]
pub struct QueryRoot(
    live_spec_refs::LiveSpecsQuery,
    alerts::AlertsQuery,
    prefixes::PrefixesQuery,
);

pub fn create_schema() -> GraphQLSchema {
    Schema::build(QueryRoot::default(), EmptyMutation, EmptySubscription).finish()
}

/// Returns the GraphQL SDL (Schema Definition Language) as a string.
/// This is used by the flow-client build script to generate types.
pub fn schema_sdl() -> String {
    let schema = create_schema();
    schema.sdl()
}

pub(crate) async fn graphql_handler(
    schema: Extension<GraphQLSchema>,
    claims: Extension<ControlClaims>,
    app_state: axum::extract::State<Arc<App>>,
    req: axum::extract::Json<async_graphql::Request>,
) -> axum::Json<async_graphql::Response> {
    let request = req.0.data(app_state.0.clone()).data(claims.0).data(
        async_graphql::dataloader::DataLoader::new(
            PgDataLoader(app_state.pg_pool.clone()),
            tokio::spawn,
        ),
    );

    let response = schema.execute(request).await;
    axum::Json(response)
}

/// Returns an HTML page for the GraphiQL interface, which allows users to
/// explore and interact with the GraphQL API. The html was copied from the
/// official example at:
/// https://github.com/graphql/graphiql/blob/0d9e51aa6452de1a1dee1ff1d1dae6df923f389f/examples/graphiql-cdn/index.html
/// The version of GraphiQL that's bundled with the `async_graphql` crate is out
/// of date, which is why we're using this html instead.
/// Changes from original:
///     1. Added default auth header
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
                url: 'http://localhost:8675/api/graphql',
              });
              const plugins = [HISTORY_PLUGIN, explorerPlugin()];

              function App() {
                return React.createElement(GraphiQL, {
                  fetcher,
                  plugins,
                  defaultEditorToolsVisibility: true,
                  // ---------- auth header customization ----------
                  defaultHeaders : (()=>{
                    const access_token = new URLSearchParams(window.location.search).get('access_token');

                    // If we have a access_token use it - otherwise make it easy to past in
                    const bearerToken = `Bearer ${access_token && access_token.length > 0 ? access_token : ''}`

                    return JSON.stringify({
                        Authorization: bearerToken,
                    });
                  })()
                  // ---------- auth header customization ----------
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

// It's critical that we preserve the order of fields within the endpoint config
// of task specs, or we'll invalidate sops encrypted configs. This property is
// guaranteed by the use of `IndexMap` to represent objects inside an
// `async_graphql::Value`. This test ensures that we'll catch it if that ever
// changes.
#[test]
fn graphql_json_values_preserves_field_order() {
    let json_str = r#"{"b": "b", "a": {"az": "az", "ab": "ab", "aZ": "aZ" }, "Z": "Z", "A": "A"}"#;
    let parsed: async_graphql::Value = serde_json::from_str(json_str).unwrap();
    let round_tripped = serde_json::to_string(&parsed).unwrap();
    assert_eq!(json_str.replace(' ', ""), round_tripped);
}
