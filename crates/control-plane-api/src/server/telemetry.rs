/// Selects the spans and events agent-api exports: entry-point spans, which declare `otel.kind`, and everything at INFO or above beneath one.
/// Work outside a request, such as controllers and automations, has no entry-point span above it and is not exported.
pub fn export_filter<S>() -> impl tracing_subscriber::layer::Filter<S>
where
    S: tracing::Subscriber + for<'lookup> tracing_subscriber::registry::LookupSpan<'lookup>,
{
    tracing_subscriber::filter::dynamic_filter_fn(
        |metadata, cx: &tracing_subscriber::layer::Context<'_, S>| {
            // This filter's context only sees spans the filter enabled.
            *metadata.level() <= tracing::Level::INFO
                && (metadata.fields().field("otel.kind").is_some() || cx.lookup_current().is_some())
        },
    )
    .with_max_level_hint(tracing_subscriber::filter::LevelFilter::INFO)
}

#[cfg(test)]
pub(crate) mod tests;
