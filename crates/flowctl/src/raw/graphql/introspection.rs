//! POD types mirroring a GraphQL introspection response, and the query which
//! produces one.
//!
//! Introspection is the GraphQL-native way to ask a server what it serves: the
//! `__schema` meta-field returns every type, field, argument, and directive of
//! the running schema. `flowctl` asks the control-plane API directly rather than
//! reading the checked-in `control-plane-api.graphql`, so that these commands
//! describe the API the user is actually talking to.
//!
//! The shapes here follow the GraphQL specification's introspection schema, so
//! they deserialize the response of [`QUERY`] and of any other spec-conformant
//! server.

/// Full-schema introspection query.
///
/// `includeDeprecated: true` is passed everywhere it's accepted so that
/// deprecated fields, enum values, input fields, and arguments are described
/// instead of silently omitted. `TypeRef` is unrolled to eight levels of
/// wrapping types, which is the depth conventionally used to cover any
/// realistic nesting of `[T!]!`.
pub const QUERY: &str = r#"
query FlowctlIntrospection {
  __schema {
    queryType { name }
    mutationType { name }
    subscriptionType { name }
    types { ...FullType }
    directives {
      name
      description
      locations
      args(includeDeprecated: true) { ...InputValue }
    }
  }
}

fragment FullType on __Type {
  kind
  name
  description
  fields(includeDeprecated: true) {
    name
    description
    args(includeDeprecated: true) { ...InputValue }
    type { ...TypeRef }
    isDeprecated
    deprecationReason
  }
  inputFields(includeDeprecated: true) { ...InputValue }
  isOneOf
  interfaces { ...TypeRef }
  enumValues(includeDeprecated: true) {
    name
    description
    isDeprecated
    deprecationReason
  }
  possibleTypes { ...TypeRef }
}

fragment InputValue on __InputValue {
  name
  description
  type { ...TypeRef }
  defaultValue
  isDeprecated
  deprecationReason
}

fragment TypeRef on __Type {
  kind
  name
  ofType {
    kind
    name
    ofType {
      kind
      name
      ofType {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType { kind name }
            }
          }
        }
      }
    }
  }
}
"#;

/// Data of an introspection response.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Data {
    #[serde(rename = "__schema")]
    pub schema: Schema,
}

/// A GraphQL schema: its root operation types, every named type, and every
/// directive.
///
/// Every optional field here defaults, because a response only carries the
/// fields its query selected: a narrower introspection query than [`QUERY`]
/// still deserializes into these types.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Schema {
    #[serde(default)]
    pub query_type: Option<NamedType>,
    #[serde(default)]
    pub mutation_type: Option<NamedType>,
    #[serde(default)]
    pub subscription_type: Option<NamedType>,
    pub types: Vec<Type>,
    #[serde(default)]
    pub directives: Vec<Directive>,
}

/// Reference to a root operation type, which is always a named object type.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct NamedType {
    pub name: String,
}

/// A named type of the schema. Which of the optional collections are populated
/// depends on `kind`: `fields` for objects and interfaces, `input_fields` for
/// input objects, `enum_values` for enums, and so on.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Type {
    pub kind: Kind,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub fields: Option<Vec<Field>>,
    #[serde(default)]
    pub input_fields: Option<Vec<InputValue>>,
    /// Set on an input object which requires that exactly one of its fields be
    /// given, and which SDL marks with `@oneOf`.
    #[serde(default)]
    pub is_one_of: Option<bool>,
    #[serde(default)]
    pub interfaces: Option<Vec<TypeRef>>,
    #[serde(default)]
    pub enum_values: Option<Vec<EnumValue>>,
    #[serde(default)]
    pub possible_types: Option<Vec<TypeRef>>,
}

/// Kind of a type. `LIST` and `NON_NULL` are wrapping kinds which only ever
/// appear within a [`TypeRef`].
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize, serde::Serialize, clap::ValueEnum,
)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Kind {
    Scalar,
    Object,
    Interface,
    Union,
    Enum,
    InputObject,
    List,
    NonNull,
}

/// A field of an object or interface type.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Field {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub args: Vec<InputValue>,
    #[serde(rename = "type")]
    pub of_type: TypeRef,
    #[serde(default)]
    pub is_deprecated: bool,
    #[serde(default)]
    pub deprecation_reason: Option<String>,
}

/// An argument of a field or directive, or a field of an input object.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct InputValue {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(rename = "type")]
    pub of_type: TypeRef,
    #[serde(default)]
    pub default_value: Option<String>,
    #[serde(default)]
    pub is_deprecated: bool,
    #[serde(default)]
    pub deprecation_reason: Option<String>,
}

/// A value of an enum type.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EnumValue {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub is_deprecated: bool,
    #[serde(default)]
    pub deprecation_reason: Option<String>,
}

/// A directive the schema understands, such as `@deprecated`.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
pub struct Directive {
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub locations: Vec<String>,
    #[serde(default)]
    pub args: Vec<InputValue>,
}

/// A use of a type, which wraps a named type in any number of `LIST` and
/// `NON_NULL` kinds. `[Foo!]!` arrives as `NON_NULL(LIST(NON_NULL(Foo)))`.
#[derive(Debug, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TypeRef {
    pub kind: Kind,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub of_type: Option<Box<TypeRef>>,
}

impl std::fmt::Display for TypeRef {
    /// Renders the reference in GraphQL type syntax, as `[Foo!]!`.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // A wrapping kind without an inner type is not spec-conformant. Render a
        // placeholder rather than panicking, so that introspecting a
        // non-conformant server still produces usable output.
        match (self.kind, &self.of_type) {
            (Kind::NonNull, Some(inner)) => write!(f, "{inner}!"),
            (Kind::List, Some(inner)) => write!(f, "[{inner}]"),
            _ => f.write_str(self.name.as_deref().unwrap_or("<unknown>")),
        }
    }
}

impl Kind {
    /// The SDL keyword which introduces a type of this kind.
    pub fn keyword(&self) -> &'static str {
        match self {
            Kind::Scalar => "scalar",
            Kind::Object => "type",
            Kind::Interface => "interface",
            Kind::Union => "union",
            Kind::Enum => "enum",
            Kind::InputObject => "input",
            // Wrapping kinds are never introduced as a named definition.
            Kind::List => "list",
            Kind::NonNull => "non-null",
        }
    }
}

impl std::fmt::Display for Kind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Kind::Scalar => "SCALAR",
            Kind::Object => "OBJECT",
            Kind::Interface => "INTERFACE",
            Kind::Union => "UNION",
            Kind::Enum => "ENUM",
            Kind::InputObject => "INPUT_OBJECT",
            Kind::List => "LIST",
            Kind::NonNull => "NON_NULL",
        })
    }
}

/// Scalars every GraphQL server defines. They carry no information for a user
/// exploring this API, so listings hide them by default.
pub const BUILT_IN_SCALARS: [&str; 5] = ["Boolean", "Float", "ID", "Int", "String"];

impl Schema {
    /// Looks up a named type, case-insensitively so that `flowctl raw graphql
    /// describe livespec` finds `LiveSpec`.
    pub fn find_type(&self, name: &str) -> Option<&Type> {
        self.types
            .iter()
            .find(|ty| ty.name.as_deref() == Some(name))
            .or_else(|| {
                self.types.iter().find(|ty| {
                    ty.name
                        .as_deref()
                        .is_some_and(|n| n.eq_ignore_ascii_case(name))
                })
            })
    }

    /// Types of the schema, sorted by name, less the introspection meta types
    /// (`__Schema`, `__Type`, ...) and built-in scalars unless `include_builtins`.
    pub fn named_types(&self, include_builtins: bool) -> Vec<&Type> {
        let mut types: Vec<&Type> = self
            .types
            .iter()
            .filter(|ty| {
                let Some(name) = ty.name.as_deref() else {
                    return false;
                };
                include_builtins || !(name.starts_with("__") || BUILT_IN_SCALARS.contains(&name))
            })
            .collect();

        types.sort_by_key(|ty| ty.name.as_deref().unwrap_or_default());
        types
    }

    /// The object type backing an operation root, if the schema defines that root.
    pub fn root_type(&self, operation: Operation) -> Option<&Type> {
        let root = match operation {
            Operation::Query => self.query_type.as_ref(),
            Operation::Mutation => self.mutation_type.as_ref(),
        };
        self.find_type(&root?.name)
    }
}

/// A root operation of the schema which `flowctl` can invoke. Subscriptions are
/// omitted because the control-plane API serves none, and because they need a
/// streaming transport rather than the unary POST used here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, clap::ValueEnum)]
#[serde(rename_all = "camelCase")]
pub enum Operation {
    Query,
    Mutation,
}

impl std::fmt::Display for Operation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Operation::Query => "query",
            Operation::Mutation => "mutation",
        })
    }
}
