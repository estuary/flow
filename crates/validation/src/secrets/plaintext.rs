//! Conservative annotation collection over a partial connector configuration.
//! Value constraints never eliminate candidates: secret injection may supply
//! missing fields or replace values which currently select a different branch.

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
    mut candidates: Vec<&'s doc::Schema>,
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

    // Deduplicate within this document location, not across locations: recursive
    // schemas must be revisited as we descend through a finite configuration.
    let mut seen = std::collections::HashSet::new();
    let mut expanded = Vec::new();

    while let Some(schema) = candidates.pop() {
        if !seen.insert(schema as *const doc::Schema) {
            continue;
        }
        expanded.push(schema);

        for kw in schema.keywords.iter() {
            match kw {
                json::schema::Keyword::Annotation { annotation }
                    if matches!(&**annotation, doc::Annotation::Secret(true)) =>
                {
                    out.push(ptr.clone());
                    return;
                }
                json::schema::Keyword::Ref { r#ref } => {
                    candidates.push(index.fetch(r#ref).expect("verified reference").0);
                }
                json::schema::Keyword::DynamicRef { dynamic_ref } => {
                    let (target, dynamic) = index.fetch(dynamic_ref).expect("verified reference");
                    candidates.push(target);
                    // Any matching dynamic anchor is an over-approximation of
                    // the possible dynamic scopes, including recursive ones.
                    // A $dynamicRef without a fragment behaves as a plain $ref.
                    if let (true, Some((_, fragment))) = (dynamic, dynamic_ref.rsplit_once('#')) {
                        candidates.extend(index.iter().filter_map(|(uri, dynamic, schema)| {
                            (dynamic && uri.rsplit_once('#').map(|(_, f)| f) == Some(fragment))
                                .then_some(schema)
                        }));
                    }
                }
                json::schema::Keyword::AllOf { all_of: schemas }
                | json::schema::Keyword::AnyOf { any_of: schemas }
                | json::schema::Keyword::OneOf { one_of: schemas } => {
                    candidates.extend(schemas.iter())
                }
                json::schema::Keyword::If { r#if: schema }
                | json::schema::Keyword::Then { then: schema }
                | json::schema::Keyword::Else { r#else: schema } => candidates.push(schema),
                json::schema::Keyword::DependentSchemas { dependent_schemas } => {
                    candidates.extend(dependent_schemas.iter().map(|(_, schema)| schema));
                }
                // `not` discards annotations; propertyNames describes keys, not
                // values. Definitions apply only when referenced.
                _ => {}
            }
        }
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
                let mut schemas = Vec::new();
                for schema in &expanded {
                    let mut matched = false;
                    let mut additional = None;
                    for kw in schema.keywords.iter() {
                        match kw {
                            json::schema::Keyword::Properties { properties } => {
                                for (property, schema) in properties.iter() {
                                    // '+' entries represent required-only properties,
                                    // which do not exclude additionalProperties.
                                    if property.as_bytes()[0] != b'+' && &property[1..] == name {
                                        matched = true;
                                        schemas.push(schema);
                                    }
                                }
                            }
                            json::schema::Keyword::PatternProperties { pattern_properties } => {
                                for (pattern, schema) in pattern_properties.iter() {
                                    if pattern.is_match(name) {
                                        matched = true;
                                        schemas.push(schema);
                                    }
                                }
                            }
                            json::schema::Keyword::AdditionalProperties {
                                additional_properties,
                            } => additional = Some(&**additional_properties),
                            // Evaluation depends on branch success, which we do
                            // not establish. Include these for every child.
                            json::schema::Keyword::UnevaluatedProperties {
                                unevaluated_properties,
                            } => schemas.push(unevaluated_properties),
                            _ => {}
                        }
                    }
                    if !matched {
                        schemas.extend(additional);
                    }
                }
                child(name.replace('~', "~0").replace('/', "~1"), value, schemas);
            }
        }
        serde_json::Value::Array(items) => {
            for (position, value) in items.iter().enumerate() {
                let mut schemas = Vec::new();
                for schema in &expanded {
                    let mut prefix = false;
                    let mut additional = None;
                    for kw in schema.keywords.iter() {
                        match kw {
                            json::schema::Keyword::PrefixItems { prefix_items } => {
                                if let Some(schema) = prefix_items.get(position) {
                                    prefix = true;
                                    schemas.push(schema);
                                }
                            }
                            json::schema::Keyword::Items { items } => additional = Some(&**items),
                            json::schema::Keyword::Contains { contains: schema }
                            | json::schema::Keyword::UnevaluatedItems {
                                unevaluated_items: schema,
                            } => schemas.push(schema),
                            _ => {}
                        }
                    }
                    if !prefix {
                        schemas.extend(additional);
                    }
                }
                child(position.to_string(), value, schemas);
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
