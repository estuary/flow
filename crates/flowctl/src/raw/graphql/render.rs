//! Pure rendering of an introspected schema: SDL text, and the row-shaped
//! summaries used by the `types` and `operations` listings.
//!
//! SDL output follows the conventions of the schema which the control-plane API
//! emits into `crates/flow-client/control-plane-api.graphql` — tab indentation,
//! block descriptions, types sorted by name, then directives, then the `schema`
//! block. Output of `flowctl raw graphql schema` is therefore close enough to
//! that file to diff against it, which is how a client learns whether its
//! generated types still match the API it's calling. The one deliberate
//! departure is that a documented argument list puts every argument on its own
//! line; `async_graphql`'s own SDL export only breaks the line before arguments
//! which carry a description, and runs the rest together.

use super::introspection::{
    Directive, EnumValue, Field, InputValue, Kind, Operation, Schema, Type, TypeRef,
};

/// Renders the whole schema as SDL. `include_builtins` also emits the
/// introspection meta types and built-in scalars.
pub fn schema(schema: &Schema, include_builtins: bool) -> String {
    let mut out = String::new();

    for ty in schema.named_types(include_builtins) {
        out.push_str(&type_def(ty));
        out.push('\n');
    }
    for directive in &schema.directives {
        out.push_str(&directive_def(directive));
    }

    // The root operation types are named `QueryRoot` / `MutationRoot` rather than
    // the defaults, so the `schema` block is required to bind them.
    out.push_str("schema {\n");
    if let Some(root) = &schema.query_type {
        out.push_str(&format!("\tquery: {}\n", root.name));
    }
    if let Some(root) = &schema.mutation_type {
        out.push_str(&format!("\tmutation: {}\n", root.name));
    }
    if let Some(root) = &schema.subscription_type {
        out.push_str(&format!("\tsubscription: {}\n", root.name));
    }
    out.push_str("}\n");

    out
}

/// Renders one named type as an SDL definition.
pub fn type_def(ty: &Type) -> String {
    let mut out = String::new();
    let name = ty.name.as_deref().unwrap_or("<unnamed>");

    description(&mut out, ty.description.as_deref(), 0);

    match ty.kind {
        Kind::Scalar => {
            out.push_str(&format!("scalar {name}\n"));
        }
        Kind::Union => {
            let members = ty
                .possible_types
                .iter()
                .flatten()
                .map(TypeRef::to_string)
                .collect::<Vec<_>>()
                .join(" | ");
            out.push_str(&format!("union {name} = {members}\n"));
        }
        Kind::Enum => {
            out.push_str(&format!("enum {name} {{\n"));
            for value in ty.enum_values.iter().flatten() {
                enum_value(&mut out, value);
            }
            out.push_str("}\n");
        }
        Kind::InputObject => {
            let one_of = if ty.is_one_of == Some(true) {
                " @oneOf"
            } else {
                ""
            };
            out.push_str(&format!("input {name}{one_of} {{\n"));
            for input in ty.input_fields.iter().flatten() {
                description(&mut out, input.description.as_deref(), 1);
                out.push_str(&format!("\t{}\n", input_value(input)));
            }
            out.push_str("}\n");
        }
        Kind::Object | Kind::Interface => {
            let implements: Vec<String> = ty
                .interfaces
                .iter()
                .flatten()
                .map(TypeRef::to_string)
                .collect();

            out.push_str(ty.kind.keyword());
            out.push_str(&format!(" {name}"));
            if !implements.is_empty() {
                out.push_str(&format!(" implements {}", implements.join(" & ")));
            }
            out.push_str(" {\n");

            for f in ty.fields.iter().flatten() {
                field(&mut out, f);
            }
            out.push_str("}\n");
        }
        // Wrapping kinds are never top-level definitions, but a non-conformant
        // server could report one. Emit something readable instead of nothing.
        Kind::List | Kind::NonNull => {
            out.push_str(&format!("# {} {name}\n", ty.kind));
        }
    }

    out
}

/// Renders a directive definition, as `directive @skip(if: Boolean!) on FIELD`.
fn directive_def(directive: &Directive) -> String {
    let mut out = String::new();
    description(&mut out, directive.description.as_deref(), 0);

    let args = if directive.args.is_empty() {
        String::new()
    } else {
        format!(
            "({})",
            directive
                .args
                .iter()
                .map(input_value)
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    out.push_str(&format!(
        "directive @{}{args} on {}\n",
        directive.name,
        directive.locations.join(" | ")
    ));
    out
}

/// Renders a field of an object or interface, with its arguments. Arguments go
/// on their own lines when any of them is documented, so that the descriptions
/// stay readable.
fn field(out: &mut String, field: &Field) {
    description(out, field.description.as_deref(), 1);

    let multiline = field.args.iter().any(|arg| arg.description.is_some());

    if field.args.is_empty() {
        out.push_str(&format!("\t{}", field.name));
    } else if multiline {
        out.push_str(&format!("\t{}(\n", field.name));
        for arg in &field.args {
            description(out, arg.description.as_deref(), 2);
            out.push_str(&format!("\t\t{}\n", input_value(arg)));
        }
        out.push_str("\t)");
    } else {
        let args = field
            .args
            .iter()
            .map(input_value)
            .collect::<Vec<_>>()
            .join(", ");
        out.push_str(&format!("\t{}({args})", field.name));
    }

    out.push_str(&format!(": {}", field.of_type));
    if field.is_deprecated {
        out.push_str(&deprecated(field.deprecation_reason.as_deref()));
    }
    out.push('\n');
}

/// Renders an argument or input-object field, as `first: Int = 10`.
fn input_value(input: &InputValue) -> String {
    let mut out = format!("{}: {}", input.name, input.of_type);

    // `default_value` arrives as a GraphQL literal, already quoted if a string.
    if let Some(default) = &input.default_value {
        out.push_str(&format!(" = {default}"));
    }
    if input.is_deprecated {
        out.push_str(&deprecated(input.deprecation_reason.as_deref()));
    }
    out
}

fn enum_value(out: &mut String, value: &EnumValue) {
    description(out, value.description.as_deref(), 1);
    out.push_str(&format!("\t{}", value.name));
    if value.is_deprecated {
        out.push_str(&deprecated(value.deprecation_reason.as_deref()));
    }
    out.push('\n');
}

fn deprecated(reason: Option<&str>) -> String {
    match reason {
        // GraphQL and JSON agree on string escaping for everything a schema can
        // hold, so serde_json produces a valid GraphQL string literal.
        Some(reason) => format!(
            " @deprecated(reason: {})",
            serde_json::to_string(reason).expect("a str always serializes")
        ),
        None => " @deprecated".to_string(),
    }
}

/// Writes a block description at `indent` tabs, or nothing when undocumented.
fn description(out: &mut String, description: Option<&str>, indent: usize) {
    let Some(description) = description else {
        return;
    };
    let tabs = "\t".repeat(indent);

    out.push_str(&format!("{tabs}\"\"\"\n"));
    for line in description.lines() {
        out.push_str(&format!("{tabs}{line}\n"));
    }
    out.push_str(&format!("{tabs}\"\"\"\n"));
}

/// One row of `flowctl raw graphql types`.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeSummary {
    pub kind: Kind,
    pub name: String,
    pub description: Option<String>,
    /// Count of fields, input fields, enum values, or union members.
    pub members: usize,
}

impl TypeSummary {
    pub fn new(ty: &Type) -> Self {
        Self {
            kind: ty.kind,
            name: ty.name.clone().unwrap_or_default(),
            description: ty.description.clone(),
            members: ty.fields.iter().flatten().count()
                + ty.input_fields.iter().flatten().count()
                + ty.enum_values.iter().flatten().count()
                + ty.possible_types.iter().flatten().count(),
        }
    }
}

impl crate::output::CliOutput for TypeSummary {
    type TableAlt = ();
    type CellValue = String;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["Kind", "Name", "Members", "Description"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        vec![
            self.kind.to_string(),
            self.name,
            self.members.to_string(),
            summarize(self.description.as_deref()),
        ]
    }
}

/// One row of `flowctl raw graphql operations`: a field of the query or mutation
/// root, which is a thing the caller can invoke.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct OperationSummary {
    pub operation: Operation,
    pub name: String,
    pub arguments: String,
    pub returns: String,
    pub description: Option<String>,
    pub deprecated: Option<String>,
}

impl OperationSummary {
    pub fn new(operation: Operation, field: &Field) -> Self {
        Self {
            operation,
            name: field.name.clone(),
            arguments: field
                .args
                .iter()
                .map(input_value)
                .collect::<Vec<_>>()
                .join(", "),
            returns: field.of_type.to_string(),
            description: field.description.clone(),
            deprecated: field.is_deprecated.then(|| {
                field
                    .deprecation_reason
                    .clone()
                    .unwrap_or_else(|| "yes".to_string())
            }),
        }
    }
}

impl crate::output::CliOutput for OperationSummary {
    type TableAlt = ();
    type CellValue = String;

    fn table_headers(_alt: Self::TableAlt) -> Vec<&'static str> {
        vec!["Operation", "Name", "Arguments", "Returns", "Description"]
    }

    fn into_table_row(self, _alt: Self::TableAlt) -> Vec<Self::CellValue> {
        let description = match &self.deprecated {
            Some(reason) => format!("DEPRECATED: {reason}"),
            None => summarize(self.description.as_deref()),
        };
        vec![
            self.operation.to_string(),
            self.name,
            self.arguments,
            self.returns,
            description,
        ]
    }
}

/// First line of a description, shortened for a table cell.
fn summarize(description: Option<&str>) -> String {
    const MAX: usize = 96;

    let Some(first) = description.and_then(|d| d.lines().find(|l| !l.trim().is_empty())) else {
        return String::new();
    };
    let first = first.trim();

    match first.char_indices().nth(MAX) {
        Some((offset, _)) => format!("{}…", &first[..offset]),
        None => first.to_string(),
    }
}

#[cfg(test)]
pub(crate) mod test {
    use super::*;
    use serde_json::json;

    fn named(kind: &str, name: &str) -> serde_json::Value {
        json!({"kind": kind, "name": name})
    }

    fn non_null(inner: serde_json::Value) -> serde_json::Value {
        json!({"kind": "NON_NULL", "ofType": inner})
    }

    fn list(inner: serde_json::Value) -> serde_json::Value {
        json!({"kind": "LIST", "ofType": inner})
    }

    /// A schema exercising each rendering path: an object with both an
    /// undocumented and a documented argument list, an interface it implements, a
    /// union, an enum holding a deprecated value, a `@oneOf` input object with
    /// defaults, a custom scalar, and the built-in and meta types which listings
    /// hide.
    pub(crate) fn schema_fixture() -> Schema {
        serde_json::from_value(json!({
            "queryType": {"name": "QueryRoot"},
            "mutationType": {"name": "MutationRoot"},
            "types": [
                // Hidden from listings unless `--all` is given.
                {"kind": "SCALAR", "name": "String"},
                {"kind": "OBJECT", "name": "__Placeholder", "fields": []},

                {
                    "kind": "SCALAR",
                    "name": "Id",
                    "description": "An opaque control-plane identifier.",
                },
                {
                    "kind": "ENUM",
                    "name": "Capability",
                    "enumValues": [
                        {"name": "read", "description": "May read."},
                        {
                            "name": "admin",
                            "isDeprecated": true,
                            "deprecationReason": "Use \"write\" instead.",
                        },
                    ],
                },
                {
                    "kind": "INPUT_OBJECT",
                    "name": "PrivateLinkConfigInput",
                    "isOneOf": true,
                    "inputFields": [
                        {"name": "aws", "type": named("INPUT_OBJECT", "AwsInput")},
                        {
                            "name": "region",
                            "description": "Region of the link.",
                            "type": non_null(named("SCALAR", "String")),
                            "defaultValue": "\"us-east-1\"",
                        },
                        {
                            "name": "zone",
                            "type": named("SCALAR", "String"),
                            "isDeprecated": true,
                            "deprecationReason": "Inferred from `region`.",
                        },
                    ],
                },
                {"kind": "INPUT_OBJECT", "name": "AwsInput", "inputFields": [
                    {"name": "serviceName", "type": non_null(named("SCALAR", "String"))},
                ]},
                {
                    "kind": "INTERFACE",
                    "name": "Node",
                    "fields": [{"name": "id", "type": non_null(named("SCALAR", "Id"))}],
                    "possibleTypes": [named("OBJECT", "LiveSpec")],
                },
                {
                    "kind": "OBJECT",
                    "name": "LiveSpec",
                    "description": "A live specification.\n\nOne per catalog name.",
                    "interfaces": [named("INTERFACE", "Node")],
                    "fields": [
                        {"name": "id", "type": non_null(named("SCALAR", "Id"))},
                        {
                            "name": "alerts",
                            "type": non_null(list(non_null(named("OBJECT", "Alert")))),
                            "args": [
                                {"name": "first", "type": named("SCALAR", "Int")},
                                {"name": "after", "type": named("SCALAR", "String")},
                            ],
                        },
                        {
                            "name": "prefixes",
                            "description": "Prefixes of this spec.",
                            "type": non_null(list(non_null(named("SCALAR", "String")))),
                            "args": [
                                {
                                    "name": "minCapability",
                                    "description": "Least capability to report.",
                                    "type": non_null(named("ENUM", "Capability")),
                                },
                                {
                                    "name": "first",
                                    "type": named("SCALAR", "Int"),
                                    "defaultValue": "10",
                                },
                            ],
                        },
                        {
                            "name": "oldName",
                            "type": named("SCALAR", "String"),
                            "isDeprecated": true,
                            "deprecationReason": "Use \"catalogName\" instead.",
                        },
                    ],
                },
                {"kind": "OBJECT", "name": "Alert", "fields": [
                    {"name": "firedAt", "type": non_null(named("SCALAR", "String"))},
                ]},
                {
                    "kind": "UNION",
                    "name": "SpecOrAlert",
                    "possibleTypes": [named("OBJECT", "LiveSpec"), named("OBJECT", "Alert")],
                },
                {"kind": "OBJECT", "name": "QueryRoot", "fields": [
                    {
                        "name": "node",
                        "description": "Resolves any node by id.",
                        "type": named("INTERFACE", "Node"),
                        "args": [{"name": "id", "type": non_null(named("SCALAR", "Id"))}],
                    },
                ]},
                {"kind": "OBJECT", "name": "MutationRoot", "fields": [
                    {
                        "name": "deleteLiveSpec",
                        "type": non_null(named("SCALAR", "Boolean")),
                        "args": [{"name": "id", "type": non_null(named("SCALAR", "Id"))}],
                    },
                ]},
            ],
            "directives": [
                {
                    "name": "deprecated",
                    "description": "Marks an element as no longer supported.",
                    "locations": ["FIELD_DEFINITION", "ENUM_VALUE"],
                    "args": [{
                        "name": "reason",
                        "type": named("SCALAR", "String"),
                        "defaultValue": "\"No longer supported\"",
                    }],
                },
                {"name": "oneOf", "locations": ["INPUT_OBJECT"]},
            ],
        }))
        .expect("the fixture matches the introspection types")
    }

    #[test]
    fn test_render_schema() {
        insta::assert_snapshot!(schema(&schema_fixture(), false));
    }

    #[test]
    fn test_render_schema_all_includes_built_ins() {
        let rendered = schema(&schema_fixture(), true);

        assert!(rendered.contains("scalar String\n"));
        assert!(rendered.contains("type __Placeholder {"));
    }

    #[test]
    fn test_summarize() {
        assert_eq!(summarize(None), "");
        assert_eq!(summarize(Some("\n\n  first line \nsecond")), "first line");
        assert_eq!(
            summarize(Some(&"x".repeat(200))),
            format!("{}…", "x".repeat(96))
        );
    }
}
