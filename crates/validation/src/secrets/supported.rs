//! Whether a connector's endpoint configuration schema supports a `secrets`
//! stanza, or must keep using `sops`.
//!
//! This is an allowlist over the raw schema, with local `$ref`s followed.
//! Clients render a form for the schema, and pull secret values out of the
//! form data into the stanza. That works only where a secret's location
//! is a plain object property, and where any union holding it is resolvable
//! once its secret values are removed.

/// Ok if every `secret: true` (or `airbyte_secret: true`) location of
/// `schema` may be supplied through a `secrets` stanza, or the first reason
/// it can't. A schema without secrets is supported.
pub fn supports_secrets(schema: &serde_json::Value) -> Result<(), UnsupportedReason> {
    check(schema, schema, false, &mut String::new(), &mut Vec::new())
}

/// Why a schema doesn't support secrets. Each `path` is the JSON pointer
/// of the offending schema location, and is empty at the schema root.
#[derive(Debug, thiserror::Error)]
pub enum UnsupportedReason {
    #[error("{}secret is not an object property", at(.path))]
    NotAProperty { path: String },
    #[error("{}secret's type doesn't include string", at(.path))]
    NotAString { path: String },
    #[error("{}secret has a non-empty default", at(.path))]
    NonEmptyDefault { path: String },
    #[error("{}secret is part of an OAuth2 flow", at(.path))]
    OAuth2 { path: String },
    #[error("{}secret is under `{keyword}`", at(.path))]
    UnderKeyword { path: String, keyword: String },
    #[error("{}cannot resolve `$ref` {reference:?}", at(.path))]
    UnresolvedRef { path: String, reference: String },
    #[error("{}`{keyword}` has {options} non-null options and no discriminator", at(.path))]
    UnionWithoutDiscriminator {
        path: String,
        keyword: String,
        options: usize,
    },
    #[error(
        "{}`{keyword}` option {option} doesn't set discriminator {tag:?} to a non-secret const or default",
        at(.path)
    )]
    DiscriminatorNotSet {
        path: String,
        keyword: String,
        option: usize,
        tag: String,
    },
}

fn at(path: &str) -> String {
    if path.is_empty() {
        String::new()
    } else {
        format!("{path}: ")
    }
}

fn check<'s>(
    root: &'s serde_json::Value,
    node: &'s serde_json::Value,
    as_property: bool,
    path: &mut String,
    refs: &mut Vec<&'s str>,
) -> Result<(), UnsupportedReason> {
    let Some(fields) = node.as_object() else {
        return Ok(()); // A boolean schema holds no secret.
    };
    if !contains_secret(root, node, &mut Vec::new()) {
        return Ok(());
    }

    if is_secret(fields) {
        if !as_property {
            return Err(UnsupportedReason::NotAProperty { path: path.clone() });
        }
        let is_string = match fields.get("type") {
            Some(serde_json::Value::String(t)) => t == "string",
            Some(serde_json::Value::Array(types)) => types.iter().any(|t| t == "string"),
            _ => false,
        };
        if !is_string {
            return Err(UnsupportedReason::NotAString { path: path.clone() });
        }
        // A default secret is a placeholder the user is meant to replace,
        // and the form can't tell it apart from a value they entered.
        if fields
            .get("default")
            .is_some_and(|d| !d.is_null() && d != "")
        {
            return Err(UnsupportedReason::NonEmptyDefault { path: path.clone() });
        }
    }
    if fields.contains_key("x-oauth2-provider") {
        return Err(UnsupportedReason::OAuth2 { path: path.clone() });
    }

    for (keyword, value) in fields {
        if !keyword_holds_secret(root, keyword, value, &mut Vec::new()) {
            continue;
        }
        let len = path.len();
        let result = match keyword.as_str() {
            "properties" => {
                value
                    .as_object()
                    .into_iter()
                    .flatten()
                    .try_for_each(|(name, schema)| {
                        let len = path.len();
                        path.push_str("/properties/");
                        path.push_str(&name.replace('~', "~0").replace('/', "~1"));
                        let result = check(root, schema, true, path, refs);
                        path.truncate(len);
                        result
                    })
            }
            "allOf" | "anyOf" | "oneOf" => {
                let options = value.as_array().map(Vec::as_slice).unwrap_or_default();
                if keyword != "allOf" {
                    check_union(root, fields, keyword, options, path)?;
                }
                options.iter().enumerate().try_for_each(|(index, schema)| {
                    let len = path.len();
                    path.push_str(&format!("/{keyword}/{index}"));
                    let result = check(root, schema, as_property, path, refs);
                    path.truncate(len);
                    result
                })
            }
            "$ref" => {
                let r#ref = value.as_str().unwrap_or_default();
                match resolve(root, r#ref) {
                    // A recursive reference is already being checked.
                    Some(_) if refs.contains(&r#ref) => Ok(()),
                    Some(target) => {
                        refs.push(r#ref);
                        let result = check(root, target, as_property, path, refs);
                        refs.pop();
                        result
                    }
                    None => Err(UnsupportedReason::UnresolvedRef {
                        path: path.clone(),
                        reference: r#ref.to_string(),
                    }),
                }
            }
            _ => Err(UnsupportedReason::UnderKeyword {
                path: path.clone(),
                keyword: keyword.clone(),
            }),
        };
        path.truncate(len);
        result?;
    }
    Ok(())
}

/// A union holding a secret must be resolvable without its secret values:
/// it has at most one non-null option, or a discriminator which every
/// non-null option sets to a non-secret `const` or `default`.
fn check_union(
    root: &serde_json::Value,
    fields: &serde_json::Map<String, serde_json::Value>,
    keyword: &str,
    options: &[serde_json::Value],
    path: &str,
) -> Result<(), UnsupportedReason> {
    let options: Vec<&serde_json::Value> = options
        .iter()
        .map(|option| follow_refs(root, option))
        .filter(|option| option.get("type").is_none_or(|t| t != "null"))
        .collect();
    if options.len() <= 1 {
        return Ok(());
    }
    let Some(tag) = fields
        .get("discriminator")
        .and_then(|d| d.get("propertyName"))
        .and_then(serde_json::Value::as_str)
    else {
        return Err(UnsupportedReason::UnionWithoutDiscriminator {
            path: path.to_string(),
            keyword: keyword.to_string(),
            options: options.len(),
        });
    };
    for (index, option) in options.into_iter().enumerate() {
        let sets_tag = option
            .get("properties")
            .and_then(|p| p.get(tag))
            .map(|p| follow_refs(root, p))
            .is_some_and(|p| {
                (p.get("const").is_some() || p.get("default").is_some())
                    && !contains_secret(root, p, &mut Vec::new())
            });
        if !sets_tag {
            return Err(UnsupportedReason::DiscriminatorNotSet {
                path: path.to_string(),
                keyword: keyword.to_string(),
                option: index,
                tag: tag.to_string(),
            });
        }
    }
    Ok(())
}

fn is_secret(fields: &serde_json::Map<String, serde_json::Value>) -> bool {
    fields.get("secret") == Some(&serde_json::Value::Bool(true))
        || fields.get("airbyte_secret") == Some(&serde_json::Value::Bool(true))
}

/// Whether `node` holds a secret, at itself or any schema it applies.
/// `refs` holds the references being followed, to stop on recursion.
fn contains_secret<'s>(
    root: &'s serde_json::Value,
    node: &'s serde_json::Value,
    refs: &mut Vec<&'s str>,
) -> bool {
    node.as_object().is_some_and(|fields| {
        is_secret(fields)
            || fields
                .iter()
                .any(|(keyword, value)| keyword_holds_secret(root, keyword, value, refs))
    })
}

fn keyword_holds_secret<'s>(
    root: &'s serde_json::Value,
    keyword: &str,
    value: &'s serde_json::Value,
    refs: &mut Vec<&'s str>,
) -> bool {
    match keyword {
        // Definitions apply only where referenced, and these hold data, not schemas.
        "$defs" | "definitions" | "const" | "default" | "enum" | "examples" => false,
        // These map names to schemas. A name is not a keyword.
        "properties" | "patternProperties" | "dependentSchemas" | "dependencies" => {
            value.as_object().is_some_and(|map| {
                map.values()
                    .any(|schema| contains_secret(root, schema, refs))
            })
        }
        "$ref" => {
            let r#ref = value.as_str().unwrap_or_default();
            if refs.contains(&r#ref) {
                return false;
            }
            // An unresolvable reference may hold a secret.
            let Some(target) = resolve(root, r#ref) else {
                return true;
            };
            refs.push(r#ref);
            let found = contains_secret(root, target, refs);
            refs.pop();
            found
        }
        _ => match value {
            serde_json::Value::Array(items) => {
                items.iter().any(|item| contains_secret(root, item, refs))
            }
            _ => contains_secret(root, value, refs),
        },
    }
}

/// Resolve a reference local to the schema document.
fn resolve<'s>(root: &'s serde_json::Value, r#ref: &str) -> Option<&'s serde_json::Value> {
    root.pointer(r#ref.strip_prefix('#')?)
}

/// The schema `node` refers to, through any chain of bare references.
fn follow_refs<'s>(
    root: &'s serde_json::Value,
    mut node: &'s serde_json::Value,
) -> &'s serde_json::Value {
    // Bounded, so a reference cycle can't loop forever.
    for _ in 0..32 {
        match node
            .get("$ref")
            .and_then(serde_json::Value::as_str)
            .and_then(|r| resolve(root, r))
        {
            Some(target) => node = target,
            None => break,
        }
    }
    node
}

#[cfg(test)]
mod test {
    #[test]
    fn allowlist() {
        let cases = [
            (
                "no_secrets",
                r#"
properties:
  host: {type: string}
  tags: {type: array, items: {type: string}}
"#,
            ),
            (
                "property_secrets",
                r#"
properties:
  password: {type: string, secret: true}
  token: {type: [string, "null"], airbyte_secret: true, default: ""}
  nested:
    allOf:
      - properties:
          apiKey: {type: string, secret: true}
"#,
            ),
            (
                "property_named_secret",
                r#"
properties:
  secret: true
  airbyte_secret: {type: string}
"#,
            ),
            (
                "null_default",
                r#"
properties:
  token: {type: string, secret: true, default: null}
"#,
            ),
            (
                "referenced_secret",
                r##"
$defs:
  password: {type: string, secret: true}
properties:
  password: {$ref: '#/$defs/password'}
"##,
            ),
            (
                "unreferenced_definition",
                r#"
$defs:
  unused:
    items: {type: string, secret: true}
definitions:
  unused: {type: integer, secret: true}
properties:
  host: {type: string}
"#,
            ),
            (
                "union_with_null",
                r#"
properties:
  credentials:
    anyOf:
      - type: object
        properties:
          password: {type: string, secret: true}
      - type: "null"
"#,
            ),
            (
                "discriminated_union",
                r##"
$defs:
  token:
    type: object
    properties:
      auth_type: {const: token}
      token: {type: string, secret: true}
properties:
  credentials:
    discriminator: {propertyName: auth_type}
    oneOf:
      - $ref: '#/$defs/token'
      - type: object
        properties:
          auth_type: {type: string, default: password}
          password: {type: string, secret: true}
"##,
            ),
            (
                "recursive",
                r##"
$defs:
  node:
    type: object
    properties:
      token: {type: string, secret: true}
      next: {$ref: '#/$defs/node'}
$ref: '#/$defs/node'
"##,
            ),
            (
                "union_without_discriminator",
                r#"
properties:
  credentials:
    oneOf:
      - properties:
          password: {type: string, secret: true}
      - properties:
          apiKey: {type: string, secret: true}
"#,
            ),
            (
                "discriminator_not_set",
                r#"
properties:
  credentials:
    discriminator: {propertyName: auth_type}
    oneOf:
      - properties:
          auth_type: {const: password}
          password: {type: string, secret: true}
      - properties:
          apiKey: {type: string, secret: true}
"#,
            ),
            (
                "secret_discriminator",
                r#"
properties:
  credentials:
    discriminator: {propertyName: key}
    anyOf:
      - properties:
          key: {type: string, const: a, secret: true}
      - properties:
          key: {type: string, const: b}
"#,
            ),
            (
                "oauth",
                r#"
properties:
  credentials:
    x-oauth2-provider: acme
    properties:
      client_secret: {type: string, secret: true}
"#,
            ),
            (
                "non_empty_default",
                r#"
properties:
  token: {type: string, secret: true, default: placeholder}
"#,
            ),
            (
                "non_string",
                r#"
properties:
  credentials: {type: object, secret: true}
"#,
            ),
            ("root", "{type: string, secret: true}"),
            (
                "array_items",
                r#"
properties:
  stores:
    type: array
    items:
      properties:
        token: {type: string, secret: true}
"#,
            ),
            (
                "pattern_properties",
                r#"
patternProperties:
  '^x': {type: string, secret: true}
"#,
            ),
            (
                "if_then",
                r#"
if:
  properties:
    kind: {const: a}
then:
  properties:
    token: {type: string, secret: true}
"#,
            ),
            (
                "dependent_schemas",
                r#"
dependentSchemas:
  user:
    properties:
      password: {type: string, secret: true}
"#,
            ),
            (
                "unresolved_reference",
                r#"
properties:
  password: {$ref: 'https://example.test/password'}
"#,
            ),
        ];
        let actual = cases.map(|(name, schema)| {
            let schema: serde_json::Value = serde_yaml::from_str(schema).unwrap();
            (
                name,
                super::supports_secrets(&schema).map_err(|err| err.to_string()),
            )
        });
        insta::assert_debug_snapshot!(actual);
    }
}
