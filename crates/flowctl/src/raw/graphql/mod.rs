//! Ad-hoc access to the control-plane GraphQL API.
//!
//! These commands are the GraphQL counterpart of `flowctl raw get` and `flowctl
//! raw rpc`: they let you call the API directly, without a purpose-built
//! `flowctl` command, and they let you ask the API to describe itself.
//!
//! `exec` posts a document you supply. The remaining commands read the schema by
//! introspection and render it: `schema` for the whole SDL, `types` and
//! `operations` for listings, and `describe` for one type.

use anyhow::Context;
use itertools::Itertools;
use std::io::Write;

mod introspection;
mod render;

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Graphql {
    #[clap(subcommand)]
    cmd: Command,
}

#[derive(Debug, clap::Subcommand)]
#[clap(rename_all = "kebab-case")]
pub enum Command {
    /// Execute a GraphQL query or mutation.
    ///
    /// The document is taken from the positional argument, from --file, or from
    /// stdin. Variables are given as a JSON object with --variables, as
    /// individual --var name=value pairs, or both.
    ///
    /// The complete response is printed, including its `errors` if the API
    /// returned any, and the command exits non-zero when it did.
    ///
    /// For example:
    ///
    ///   flowctl raw graphql exec 'query { alertTypes { alertType } }'
    ///
    ///   flowctl raw graphql exec --var prefix=acmeCo/ \
    ///     'query Q($prefix: Prefix!) { prefixes(by: {prefix: $prefix}) { edges { node { prefix } } } }'
    Exec(Exec),
    /// Print the schema which the API serves.
    ///
    /// The schema is read by introspection, so it describes the API this profile
    /// is pointed at. SDL output follows the conventions of the schema checked in
    /// at crates/flow-client/control-plane-api.graphql, so the two can be diffed
    /// to find where a client's generated types have fallen behind the API.
    Schema(Schema),
    /// List the types of the schema.
    Types(Types),
    /// Print the definition of one type, as SDL.
    Describe(Describe),
    /// List the queries and mutations which the API serves.
    Operations(Operations),
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Exec {
    /// GraphQL document to execute.
    ///
    /// Read from stdin when omitted, or when it is `-`.
    document: Option<String>,
    /// Path of a file holding the GraphQL document to execute.
    #[clap(long, conflicts_with = "document")]
    file: Option<std::path::PathBuf>,
    /// Variables of the operation, as a JSON object.
    #[clap(long)]
    variables: Option<String>,
    /// A single variable, as `name=value`, which may be repeated.
    ///
    /// The value is used as JSON if it parses as JSON, and as a string
    /// otherwise: `--var first=10` passes a number, and `--var prefix=acmeCo/`
    /// passes a string. Pairs given here override --variables.
    #[clap(long = "var", value_parser = super::parse_key_val::<String, String>, number_of_values = 1)]
    var: Vec<(String, String)>,
    /// Name of the operation to execute.
    ///
    /// Required when the document defines more than one operation.
    #[clap(long)]
    operation_name: Option<String>,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Schema {
    /// Form in which to print the schema.
    #[clap(long, value_enum, default_value = "sdl")]
    format: SchemaFormat,
    /// Also include built-in scalars and the `__`-prefixed introspection types.
    #[clap(long)]
    all: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
#[clap(rename_all = "kebab-case")]
pub enum SchemaFormat {
    /// GraphQL schema definition language.
    Sdl,
    /// The introspection response itself, which is what other GraphQL tooling
    /// consumes.
    Introspection,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Types {
    /// Only list types of this kind.
    #[clap(long, value_enum)]
    kind: Option<introspection::Kind>,
    /// Only list types whose name contains this substring, case-insensitively.
    #[clap(long)]
    search: Option<String>,
    /// Also list built-in scalars and the `__`-prefixed introspection types.
    #[clap(long)]
    all: bool,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Describe {
    /// Name of the type to describe, such as `QueryRoot` or `LiveSpec`.
    ///
    /// Matched case-insensitively when there's no exact match.
    name: String,
}

#[derive(Debug, clap::Args)]
#[clap(rename_all = "kebab-case")]
pub struct Operations {
    /// Limit the listing to one root: `query` or `mutation`.
    #[clap(value_enum)]
    operation: Option<introspection::Operation>,
    /// Only list operations whose name contains this substring, case-insensitively.
    #[clap(long)]
    search: Option<String>,
}

pub async fn do_graphql(ctx: &mut crate::CliContext, args: &Graphql) -> anyhow::Result<()> {
    match &args.cmd {
        Command::Exec(exec) => do_exec(ctx, exec).await,
        Command::Schema(schema) => do_schema(ctx, schema).await,
        Command::Types(types) => do_types(ctx, types).await,
        Command::Describe(describe) => do_describe(ctx, describe).await,
        Command::Operations(operations) => do_operations(ctx, operations).await,
    }
}

/// A GraphQL request, as posted to the API.
#[derive(Debug, serde::Serialize)]
struct Request<'a> {
    query: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    variables: Option<serde_json::Value>,
    #[serde(rename = "operationName", skip_serializing_if = "Option::is_none")]
    operation_name: Option<&'a str>,
}

async fn do_exec(ctx: &mut crate::CliContext, exec: &Exec) -> anyhow::Result<()> {
    let document = read_document(exec)?;
    if document.trim().is_empty() {
        anyhow::bail!("no GraphQL document was provided");
    }
    let variables = build_variables(exec.variables.as_deref(), &exec.var)?;

    // The whole response envelope is wanted here, errors included, so it's
    // deserialized as an opaque JSON value rather than through `post_graphql`.
    let response: serde_json::Value = crate::graphql::agent_unary(
        &ctx.rest,
        ctx.access_token().as_deref(),
        crate::graphql::GRAPHQL_PATH,
        &Request {
            query: &document,
            variables,
            operation_name: exec.operation_name.as_deref(),
        },
    )
    .await
    .context("executing the GraphQL request")?;

    print_json_or_yaml(ctx, &response)?;

    // Signal failure through the exit code, since a GraphQL error arrives with an
    // HTTP 200 and would otherwise look like success to a calling script.
    let errors = response
        .get("errors")
        .and_then(serde_json::Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or_default();

    if !errors.is_empty() {
        anyhow::bail!(
            "the API returned {} GraphQL error(s): {}",
            errors.len(),
            errors
                .iter()
                .map(|error| error
                    .get("message")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("(no message)"))
                .format("; ")
        );
    }
    Ok(())
}

async fn do_schema(ctx: &mut crate::CliContext, args: &Schema) -> anyhow::Result<()> {
    if let SchemaFormat::Introspection = args.format {
        let response = introspect_raw(ctx).await?;
        return print_json_or_yaml(ctx, &response);
    }
    let schema = introspect(ctx).await?;

    let mut stdout = std::io::stdout().lock();
    stdout.write_all(render::schema(&schema, args.all).as_bytes())?;
    Ok(())
}

async fn do_types(ctx: &mut crate::CliContext, args: &Types) -> anyhow::Result<()> {
    let schema = introspect(ctx).await?;

    let rows: Vec<render::TypeSummary> = schema
        .named_types(args.all)
        .into_iter()
        .filter(|ty| args.kind.is_none_or(|kind| kind == ty.kind))
        .filter(|ty| matches_search(ty.name.as_deref(), args.search.as_deref()))
        .map(render::TypeSummary::new)
        .collect();

    if rows.is_empty() {
        anyhow::bail!("no types of the schema match this filter");
    }
    ctx.write_all(rows, ())
}

async fn do_describe(ctx: &mut crate::CliContext, args: &Describe) -> anyhow::Result<()> {
    let schema = introspect(ctx).await?;

    let Some(ty) = schema.find_type(&args.name) else {
        // Point at the near misses, since type names are long and camel-cased.
        let similar = similar_names(&schema, &args.name);

        if similar.is_empty() {
            anyhow::bail!(
                "the schema has no type named {:?}. Run `flowctl raw graphql types` to list them",
                args.name
            );
        }
        anyhow::bail!(
            "the schema has no type named {:?}. Similar names: {}",
            args.name,
            similar.iter().format(", ")
        );
    };

    let mut stdout = std::io::stdout().lock();
    stdout.write_all(render::type_def(ty).as_bytes())?;
    Ok(())
}

async fn do_operations(ctx: &mut crate::CliContext, args: &Operations) -> anyhow::Result<()> {
    let schema = introspect(ctx).await?;

    let operations = match args.operation {
        Some(operation) => vec![operation],
        None => vec![
            introspection::Operation::Query,
            introspection::Operation::Mutation,
        ],
    };

    let mut rows: Vec<render::OperationSummary> = Vec::new();
    for operation in operations {
        let Some(root) = schema.root_type(operation) else {
            tracing::debug!(%operation, "the schema defines no root type for this operation");
            continue;
        };
        rows.extend(
            root.fields
                .iter()
                .flatten()
                .filter(|field| matches_search(Some(&field.name), args.search.as_deref()))
                .map(|field| render::OperationSummary::new(operation, field)),
        );
    }

    if rows.is_empty() {
        anyhow::bail!("no operations of the schema match this filter");
    }
    ctx.write_all(rows, ())
}

/// Reads the schema of the API by introspection.
async fn introspect(ctx: &crate::CliContext) -> anyhow::Result<introspection::Schema> {
    let response: graphql_client::Response<introspection::Data> = crate::graphql::agent_unary(
        &ctx.rest,
        ctx.access_token().as_deref(),
        crate::graphql::GRAPHQL_PATH,
        &Request {
            query: introspection::QUERY,
            variables: None,
            operation_name: None,
        },
    )
    .await
    .context("introspecting the GraphQL API")?;

    if let Some(errors) = response.errors.filter(|errors| !errors.is_empty()) {
        anyhow::bail!("introspection failed: [{}]", errors.iter().format(", "));
    }
    Ok(response
        .data
        .context("the introspection response has no data")?
        .schema)
}

/// Reads the schema of the API by introspection, without interpreting the
/// response. Backs `schema --format json`, which is the form other GraphQL
/// tooling consumes.
async fn introspect_raw(ctx: &crate::CliContext) -> anyhow::Result<serde_json::Value> {
    crate::graphql::agent_unary(
        &ctx.rest,
        ctx.access_token().as_deref(),
        crate::graphql::GRAPHQL_PATH,
        &Request {
            query: introspection::QUERY,
            variables: None,
            operation_name: None,
        },
    )
    .await
    .context("introspecting the GraphQL API")
}

fn read_document(exec: &Exec) -> anyhow::Result<String> {
    if let Some(path) = &exec.file {
        return std::fs::read_to_string(path)
            .with_context(|| format!("reading {}", path.display()));
    }
    match exec.document.as_deref() {
        Some("-") | None => {
            let mut document = String::new();
            std::io::Read::read_to_string(&mut std::io::stdin().lock(), &mut document)
                .context("reading the GraphQL document from stdin")?;
            Ok(document)
        }
        Some(document) => Ok(document.to_string()),
    }
}

/// Merges `--variables` with repeated `--var name=value` pairs, which take
/// precedence. A pair's value is used as JSON when it parses as JSON, and as a
/// string otherwise, so that both `--var first=10` and `--var prefix=acmeCo/`
/// pass the value the caller intends.
fn build_variables(
    variables: Option<&str>,
    var: &[(String, String)],
) -> anyhow::Result<Option<serde_json::Value>> {
    let mut merged = match variables {
        Some(variables) => match serde_json::from_str(variables).context("parsing --variables")? {
            serde_json::Value::Object(object) => object,
            other => anyhow::bail!("--variables must be a JSON object, but is {other}"),
        },
        None => serde_json::Map::new(),
    };

    for (name, value) in var {
        let value = serde_json::from_str(value)
            .unwrap_or_else(|_| serde_json::Value::String(value.clone()));
        merged.insert(name.clone(), value);
    }

    if merged.is_empty() {
        return Ok(None);
    }
    Ok(Some(serde_json::Value::Object(merged)))
}

/// Names of `schema`'s types which are similar to `name`, for the error message
/// of a failed lookup.
///
/// Containment is tested in both directions, so that an under-specified guess
/// (`invite`) and an over-specified one (`InviteLinkResult`) both surface
/// `InviteLink`. The reverse direction is limited to candidates of at least four
/// characters, because a short name such as `Id` is a substring of too many
/// guesses to be a useful suggestion.
fn similar_names<'a>(schema: &'a introspection::Schema, name: &str) -> Vec<&'a str> {
    const MIN_CONTAINED: usize = 4;
    let name = name.to_lowercase();

    schema
        .named_types(false)
        .into_iter()
        .filter_map(|ty| ty.name.as_deref())
        .filter(|candidate| {
            let candidate = candidate.to_lowercase();
            candidate.contains(&name)
                || (candidate.len() >= MIN_CONTAINED && name.contains(&candidate))
        })
        .take(10)
        .collect()
}

fn matches_search(name: Option<&str>, search: Option<&str>) -> bool {
    match (name, search) {
        (_, None) => true,
        (Some(name), Some(search)) => name.to_lowercase().contains(&search.to_lowercase()),
        (None, Some(_)) => false,
    }
}

/// Prints a GraphQL response or introspection payload.
///
/// JSON is the default because it's the encoding GraphQL itself uses, and it's
/// what a pipe into `jq` expects. Only an explicit `--output yaml` changes it:
/// there are no fixed columns to build a table from, and the usual "YAML when
/// stdout isn't a terminal" default would otherwise break those pipes.
fn print_json_or_yaml(
    ctx: &mut crate::CliContext,
    value: &serde_json::Value,
) -> anyhow::Result<()> {
    let mut stdout = std::io::stdout().lock();

    match ctx.output.output {
        Some(crate::output::OutputType::Yaml) => serde_yaml::to_writer(&mut stdout, value)?,
        _ => serde_json::to_writer_pretty(&mut stdout, value)?,
    }
    stdout.write_all(b"\n")?;
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    fn var(name: &str, value: &str) -> (String, String) {
        (name.to_string(), value.to_string())
    }

    #[test]
    fn test_build_variables() {
        // No inputs at all omits `variables` from the request.
        assert_eq!(build_variables(None, &[]).unwrap(), None);

        // A `--var` value is JSON when it parses as JSON, and a string otherwise.
        let vars = [
            var("first", "10"),
            var("prefix", "acmeCo/"),
            var("closed", "true"),
            var("filter", r#"{"catalogPrefix": {"startsWith": "acmeCo/"}}"#),
            var("quoted", r#""10""#),
        ];
        insta::assert_json_snapshot!(build_variables(None, &vars).unwrap());

        // `--var` pairs are merged over `--variables`.
        insta::assert_json_snapshot!(
            build_variables(
                Some(r#"{"first": 5, "prefix": "wileyCo/"}"#),
                &[var("first", "10")]
            )
            .unwrap()
        );

        // `--variables` must hold an object, and must be valid JSON.
        insta::assert_snapshot!(
            build_variables(Some("[1, 2]"), &[]).unwrap_err(),
            @"--variables must be a JSON object, but is [1,2]"
        );
        insta::assert_snapshot!(
            build_variables(Some("{"), &[]).unwrap_err(),
            @"parsing --variables"
        );
    }

    #[test]
    fn test_similar_names() {
        let schema = render::test::schema_fixture();

        // An over-specified guess finds the name it extends, and an
        // under-specified one finds every name containing it.
        assert_eq!(similar_names(&schema, "LiveSpecFoo"), vec!["LiveSpec"]);
        assert_eq!(
            similar_names(&schema, "input"),
            vec!["AwsInput", "PrivateLinkConfigInput"]
        );

        // `Id` is too short to offer merely because the guess contains it.
        assert!(similar_names(&schema, "Idle").is_empty());
        assert!(similar_names(&schema, "Zebra").is_empty());
    }

    #[test]
    fn test_matches_search() {
        assert!(matches_search(Some("LiveSpec"), None));
        assert!(matches_search(Some("LiveSpec"), Some("livespec")));
        assert!(matches_search(Some("LiveSpecConnection"), Some("Spec")));
        assert!(!matches_search(Some("LiveSpec"), Some("alert")));
        assert!(!matches_search(None, Some("alert")));
    }
}
