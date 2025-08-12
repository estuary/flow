#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("parameter {param} not found: did you mean {closest} ?")]
    ParamNotFound { param: String, closest: String },
    #[error("failed to bind parameter {encoding} (field {})", .param.projection.field)]
    BindingError {
        encoding: String,
        param: Param,
        #[source]
        err: rusqlite::Error,
    },
    #[error("SQL block contains illegal NULL characters")]
    NullStringError(#[from] std::ffi::NulError),
    #[error(
        "SQL block has ambiguous trailing non-whitespace content without a closing ';' semicolon: {trailing}"
    )]
    BlockTrailingContent { trailing: String },
    #[error("failed to prepare query for invocation: {query}")]
    Prepare {
        query: String,
        #[source]
        err: rusqlite::Error,
    },
    #[error(transparent)]
    Extractor(#[from] extractors::Error),

    // rusqlite does a pretty good job of showing context in its errors.
    #[error(transparent)]
    Rusqlite(#[from] rusqlite::Error),
}

mod connector;
mod dbutil;
mod lambda;
mod param;
mod validate;

pub use connector::connector;
pub use lambda::Lambda;
pub use param::Param;
use validate::{do_validate, parse_validate};

fn is_url_to_generate(statement: &str) -> bool {
    !statement.chars().any(char::is_whitespace)
}

// Configuration of the connector.
#[derive(serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(default)]
    migrations: Vec<String>,
}

#[derive(Debug)]
pub struct Transform {
    name: String,
    source: String,
    block: String,
    params: Vec<Param>,
}

#[cfg(test)]
fn test_param(
    field: &str,
    ptr: &str,
    is_format_integer: bool,
    is_format_number: bool,
    is_base64: bool,
) -> Param {
    use proto_flow::flow;

    Param::new(&flow::Projection {
        field: field.to_string(),
        ptr: ptr.to_string(),
        inference: Some(flow::Inference {
            string: Some(flow::inference::String {
                format: if is_format_integer {
                    "integer"
                } else if is_format_number {
                    "number"
                } else {
                    ""
                }
                .to_string(),
                content_encoding: if is_base64 { "base64" } else { "" }.to_string(),
                ..Default::default()
            }),
            default_json: "\"the default\"".to_string().into(),
            ..Default::default()
        }),
        ..Default::default()
    })
    .unwrap()
}
