use async_graphql::extensions as ext;
use tracing::Instrument as _;

/// Traces each GraphQL request and its root fields.
pub(super) struct Tracing;

impl ext::ExtensionFactory for Tracing {
    fn create(&self) -> std::sync::Arc<dyn ext::Extension> {
        std::sync::Arc::new(RequestTrace(tracing::info_span!(
            "graphql.request",
            graphql.operation.name = tracing::field::Empty,
        )))
    }
}

struct RequestTrace(tracing::Span);

#[async_trait::async_trait]
impl ext::Extension for RequestTrace {
    async fn request(
        &self,
        ctx: &ext::ExtensionContext<'_>,
        next: ext::NextRequest<'_>,
    ) -> async_graphql::Response {
        let response = next.run(ctx).instrument(self.0.clone()).await;
        // An `error` field on an error event becomes an `exception` event and marks the span as failed.
        for error in &response.errors {
            let code = match error.extensions.as_ref().and_then(|ext| ext.get("code")) {
                Some(async_graphql::Value::String(code)) => Some(code.as_str()),
                _ => None,
            };
            tracing::error!(
                parent: &self.0,
                error = %error.message,
                graphql.error.code = code,
                graphql.error.path = %serde_json::json!(error.path),
            );
        }
        response
    }

    async fn execute(
        &self,
        ctx: &ext::ExtensionContext<'_>,
        operation_name: Option<&str>,
        next: ext::NextExecute<'_>,
    ) -> async_graphql::Response {
        if let Some(name) = operation_name {
            // GraphQL names are ASCII but unbounded in length.
            self.0
                .record("graphql.operation.name", name.get(..128).unwrap_or(name));
        }
        next.run(ctx, operation_name).await
    }

    async fn resolve(
        &self,
        ctx: &ext::ExtensionContext<'_>,
        info: ext::ResolveInfo<'_>,
        next: ext::NextResolve<'_>,
    ) -> async_graphql::ServerResult<Option<async_graphql::Value>> {
        if info.path_node.parent.is_some() || info.name.starts_with("__") {
            return next.run(ctx, info).await;
        }
        let span = tracing::info_span!(
            "graphql.resolve",
            otel.name = %format_args!("{}.{}", info.parent_type, info.name),
        );
        next.run(ctx, info).instrument(span).await
    }
}

#[cfg(test)]
mod tests;
