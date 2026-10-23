//! Conservative annotation collection over a partial connector configuration.
//! Value constraints never eliminate candidates: secret injection may supply
//! missing fields or replace values which currently select a different branch.

use json::schema::applicable;

pub(super) fn find(schema: &[u8], config: &serde_json::Value) -> anyhow::Result<Vec<String>> {
    let schema = doc::validation::build_bundle(schema)?;

    let mut builder = doc::SchemaIndexBuilder::new();
    builder.add(&schema)?;
    builder.verify_references()?;
    let index = builder.into_index();

    let mut out = Vec::new();
    walk(vec![&schema], &index, config, &mut String::new(), &mut out);
    Ok(out)
}

fn walk<'s>(
    candidates: Vec<&'s doc::Schema>,
    index: &doc::SchemaIndex<'s>,
    node: &serde_json::Value,
    ptr: &mut String,
    out: &mut Vec<String>,
) {
    // `null` and empty containers hold no value to leak, and a secret merged
    // into any of them yields the same configuration as if they were absent.
    let is_empty = match node {
        serde_json::Value::Null => true,
        serde_json::Value::Object(fields) => fields.is_empty(),
        serde_json::Value::Array(items) => items.is_empty(),
        _ => false,
    };
    if is_empty || candidates.is_empty() {
        return;
    }

    let expanded = applicable::expand(candidates, index);
    let is_secret = expanded.iter().any(|schema| {
        schema.keywords.iter().any(|kw| {
            matches!(kw, json::schema::Keyword::Annotation { annotation }
                if matches!(&**annotation, doc::Annotation::Secret(true)))
        })
    });
    if is_secret {
        out.push(ptr.clone());
        return;
    }

    let mut child = |token: String, value: &serde_json::Value, schemas| {
        let len = ptr.len();
        ptr.push('/');
        ptr.push_str(&token);
        walk(schemas, index, value, ptr, out);
        ptr.truncate(len);
    };
    match node {
        serde_json::Value::Object(fields) => {
            for (name, value) in fields {
                let token = name.replace('~', "~0").replace('/', "~1");
                child(token, value, applicable::property(&expanded, name));
            }
        }
        serde_json::Value::Array(items) => {
            for (position, value) in items.iter().enumerate() {
                child(
                    position.to_string(),
                    value,
                    applicable::item(&expanded, position),
                );
            }
        }
        _ => {}
    }
}

#[cfg(test)]
mod test {
    #[test]
    fn partial_configs() {
        let cases = [
            (
                "branches",
                r#"
oneOf:
  - properties:
      credentials:
        properties:
          password: {secret: true}
    required: [missing]
  - properties:
      credentials:
        properties:
          token: {airbyte_secret: true}
          password: {secret: false}
        required: [user]
properties:
  credentials:
    type: string
"#,
                r#"{"credentials":{"password":"invented","token":"invented"}}"#,
            ),
            (
                "patterns_and_additional",
                r#"
allOf:
  - properties:
      public: {}
    required: [requiredOnly]
    patternProperties:
      '^x': {}
      'x$': {secret: true}
    additionalProperties: {secret: true}
  - properties:
      crossBranch: {}
"#,
                r#"{"public":1,"xx":2,"requiredOnly":3,"crossBranch":4,"a~/b":5}"#,
            ),
            (
                "arrays",
                r#"
properties:
  tuple:
    prefixItems: [{}, {secret: true}]
    items: {secret: true}
  contains:
    contains: {type: integer, secret: true}
  unevaluated:
    items: {}
    unevaluatedItems: {secret: true}
"#,
                r#"{"tuple":[1,2,3],"contains":["mismatch"],"unevaluated":[false]}"#,
            ),
            (
                "conditionals",
                r#"
if:
  properties:
    predicate: {secret: true}
then:
  properties:
    thenValue: {secret: true}
else:
  properties:
    elseValue: {secret: true}
dependentSchemas:
  missing:
    properties:
      dependent: {secret: true}
"#,
                r#"{"predicate":1,"thenValue":2,"elseValue":3,"dependent":4}"#,
            ),
            (
                "unevaluated_properties",
                r#"
properties:
  known: {}
unevaluatedProperties: {secret: true}
"#,
                r#"{"known":1,"unknown":2}"#,
            ),
            (
                "recursive",
                r##"
$defs:
  node:
    allOf: [{$ref: '#/$defs/node'}]
    properties:
      token: {secret: true}
      next: {$ref: '#/$defs/node'}
$ref: '#/$defs/node'
"##,
                r#"{"token":1,"next":{"token":2,"next":{"token":3}}}"#,
            ),
            (
                "dynamic",
                r##"
$id: https://example.test/root
$defs:
  base:
    $id: base
    $dynamicAnchor: node
  override:
    $id: override
    $dynamicAnchor: node
    properties:
      token: {secret: true}
$dynamicRef: 'base#node'
properties:
  plain: {$dynamicRef: 'override'}
"##,
                r#"{"token":1,"plain":{"token":2}}"#,
            ),
            (
                "ignored_applications",
                r#"
$defs:
  unused: {secret: true}
not: {secret: true}
propertyNames: {secret: true}
properties:
  public: {secret: false}
"#,
                r#"{"public":1}"#,
            ),
            (
                "parents_null_and_missing",
                r#"
properties:
  parent:
    secret: true
    properties:
      token: {secret: true}
  "null": {secret: true}
  missing: {secret: true}
  emptyObject: {secret: true}
  emptyArray: {secret: true}
  emptyString: {secret: true}
"#,
                r#"{"parent":{"token":1},"null":null,"emptyObject":{},"emptyArray":[],"emptyString":""}"#,
            ),
            ("root", "secret: true", r#"{"any":1}"#),
            ("empty_root", "secret: true", "{}"),
        ];
        let actual = cases.map(|(name, schema, config)| {
            let schema: serde_json::Value = serde_yaml::from_str(schema).unwrap();
            let config = serde_json::from_str(config).unwrap();
            (
                name,
                super::find(&serde_json::to_vec(&schema).unwrap(), &config).unwrap(),
            )
        });
        insta::assert_debug_snapshot!(actual);
    }

    #[test]
    fn unresolved_reference() {
        let err =
            super::find(br##"{"$ref":"#/$defs/missing"}"##, &serde_json::json!({})).unwrap_err();
        insta::assert_snapshot!(err.to_string());
    }
}
