use anyhow::Context;
use proto_flow::capture::response;

/// Collection fields of one or more discovered bindings.
#[derive(Debug, Clone, PartialEq)]
pub struct Collection {
    /// Normalized recommended name of the collection.
    pub recommended_name: String,
    pub document_schema: models::Schema,
    pub key: models::CompositeKey,
    pub is_fallback_key: bool,
}

/// A discovered binding, which references its Collection by index.
#[derive(Debug, Clone, PartialEq)]
pub struct Binding {
    pub resource_config_json: bytes::Bytes,
    pub resource_path: Vec<String>,
    pub disable: bool,
    pub collection: usize,
}

/// Discovered bindings and their collections.
#[derive(Debug, Default, Clone, PartialEq)]
pub struct Discovered {
    /// Bindings, ordered on their recommended names as the connector sent them,
    /// and then on their resource paths.
    pub bindings: Vec<Binding>,
    /// Collections referenced by bindings.
    pub collections: Vec<Collection>,
}

/// Parse a connector's Discovered response into Discovered.
///
/// Parsing resolves the response's form. An inline binding has a collection
/// of its own. A `linked_collections` entry is one collection, shared by
/// the bindings which reference it. Recommended names are normalized, and
/// bindings of equal names are not grouped: `merge_collections` merges
/// bindings by their target collection.
pub fn parse(discovered: response::Discovered) -> anyhow::Result<Discovered> {
    let response::Discovered {
        bindings,
        linked_collections,
    } = discovered;

    // No capture may publish more bindings than this, and bounding the
    // response also bounds the work of merging it.
    let limit = validation::MAX_BINDINGS_INDIRECT_SPECS;
    if bindings.len() > limit {
        anyhow::bail!(
            "connector protocol error: Discovered response has {} bindings, \
             which exceeds the limit of {limit}",
            bindings.len(),
        );
    }
    if linked_collections.len() > limit {
        anyhow::bail!(
            "connector protocol error: Discovered response has {} `linkedCollections`, \
             which exceeds the limit of {limit}",
            linked_collections.len(),
        );
    }

    // In indirect form, each `linked_collections` entry is a collection.
    // Otherwise, each binding inlines a collection of its own.
    let indirect = !linked_collections.is_empty();
    let mut sources = linked_collections;
    let mut resolved = Vec::with_capacity(bindings.len());

    for (index, mut binding) in bindings.into_iter().enumerate() {
        let source = if indirect {
            if !binding.recommended_name.is_empty()
                || !binding.document_schema_json.is_empty()
                || !binding.key.is_empty()
                || binding.is_fallback_key
            {
                anyhow::bail!(
                    "connector protocol error: Discovered binding {index} has inline collection fields, \
                     but the response uses `linkedCollections`"
                );
            }
            let source = binding.collection_index as usize;
            if source >= sources.len() {
                anyhow::bail!(
                    "connector protocol error: Discovered binding {index} has `collectionIndex` {source}, \
                     but the response has only {} `linkedCollections`",
                    sources.len()
                );
            }
            source
        } else {
            if binding.collection_index != 0 {
                anyhow::bail!(
                    "connector protocol error: Discovered binding {index} has a `collectionIndex`, \
                     but the response has no `linkedCollections`"
                );
            }
            sources.push(response::discovered::Collection {
                recommended_name: std::mem::take(&mut binding.recommended_name),
                document_schema_json: std::mem::take(&mut binding.document_schema_json),
                key: std::mem::take(&mut binding.key),
                is_fallback_key: binding.is_fallback_key,
            });
            sources.len() - 1
        };
        resolved.push((source, binding));
    }

    let collections = sources
        .iter()
        .map(|source| {
            let recommended_name = normalize_recommended_name(&source.recommended_name);
            if recommended_name.is_empty() {
                anyhow::bail!(
                    "connector protocol error: recommended name {:?} has no characters \
                     which are valid in a collection name",
                    source.recommended_name,
                );
            }
            let document_schema = models::Schema::new(
                serde_json::from_slice::<models::RawValue>(&source.document_schema_json)
                    .with_context(|| {
                        format!(
                            "parsing document schema of discovered collection {recommended_name}"
                        )
                    })?,
            );
            Ok(Collection {
                recommended_name,
                document_schema,
                key: models::CompositeKey::new(
                    source
                        .key
                        .iter()
                        .map(models::JsonPointer::new)
                        .collect::<Vec<_>>(),
                ),
                is_fallback_key: source.is_fallback_key,
            })
        })
        .collect::<anyhow::Result<Vec<_>>>()?;

    // Order bindings on their recommended names and then resource paths,
    // so a capture has consistent binding order across discovers no matter
    // the order of the connector's response. Bindings of equal names fan in,
    // and their order determines which provides the schema.
    resolved.sort_by(|(l, l_binding), (r, r_binding)| {
        sources[*l]
            .recommended_name
            .cmp(&sources[*r].recommended_name)
            .then_with(|| l_binding.resource_path.cmp(&r_binding.resource_path))
    });

    let bindings = resolved
        .into_iter()
        .map(|(collection, binding)| Binding {
            resource_config_json: binding.resource_config_json,
            resource_path: binding.resource_path,
            disable: binding.disable,
            collection,
        })
        .collect();

    Ok(Discovered {
        bindings,
        collections,
    })
}

/// Resolve a Discovered response into inline bindings, in response order
/// and with recommended names as the connector sent them.
pub fn inline_bindings(
    discovered: response::Discovered,
) -> anyhow::Result<Vec<response::discovered::Binding>> {
    let response::Discovered {
        mut bindings,
        linked_collections,
    } = discovered;

    if linked_collections.is_empty() {
        return Ok(bindings);
    }
    for (index, binding) in bindings.iter_mut().enumerate() {
        let Some(linked) = linked_collections.get(binding.collection_index as usize) else {
            anyhow::bail!(
                "connector protocol error: Discovered binding {index} has `collectionIndex` {}, \
                 but the response has only {} `linkedCollections`",
                binding.collection_index,
                linked_collections.len()
            );
        };
        binding.recommended_name = linked.recommended_name.clone();
        binding.document_schema_json = linked.document_schema_json.clone();
        binding.key = linked.key.clone();
        binding.is_fallback_key = linked.is_fallback_key;
        binding.collection_index = 0;
    }
    Ok(bindings)
}

/// Normalize a connector's recommended name into a valid collection name suffix.
fn normalize_recommended_name(name: &str) -> String {
    use itertools::Itertools;
    let mut parts = models::Collection::regex()
        .find_iter(name)
        .map(|m| models::collate::normalize(m.as_str().chars()).collect::<String>());

    parts.join("_")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_response_parsing() {
        let discovered: response::Discovered = serde_json::from_value(json!({
            "bindings": [
                {
                    "recommendedName": "some greetings!",
                    "resourceConfig": {
                        "stream": "greetings",
                        "syncMode": "incremental"
                    },
                    "documentSchema": {
                        "type": "object",
                        "properties": {
                            "count": { "type": "integer" },
                            "message": { "type": "string" }
                        },
                        "required": [ "count", "message" ]
                    },
                    "key": [ "/count" ]
                },
                {
                    "recommendedName": "frogs",
                    "resourceConfig": {
                        "stream": "greetings",
                        "syncMode": "incremental"
                    },
                    "documentSchema": {
                        "type": "object",
                        "properties": {
                            "croak": { "type": "string" }
                        },
                        "required": [ "croak" ]
                    },
                    "key": [ "/croak" ],
                    "disable": true
                }
            ]
        }))
        .unwrap();

        insta::assert_debug_snapshot!(parse(discovered).unwrap());
    }

    #[test]
    fn test_inline_and_indirect_forms_are_equivalent() {
        let inline: response::Discovered = serde_json::from_value(json!({
            "bindings": [
                { "recommendedName": "Shared", "resourceConfig": {"t": "a"}, "resourcePath": ["a"],
                  "documentSchema": {"const": "shared"}, "key": ["/id"] },
                { "recommendedName": "Other", "resourceConfig": {"t": "z"}, "resourcePath": ["z"],
                  "documentSchema": {"const": "other"}, "key": ["/id"], "isFallbackKey": true, "disable": true },
                { "recommendedName": "Shared!", "resourceConfig": {"t": "b"}, "resourcePath": ["b"],
                  "documentSchema": {"const": "shared"}, "key": ["/id"] },
                { "recommendedName": "Shared", "resourceConfig": {"t": "c"}, "resourcePath": ["c"],
                  "documentSchema": {"const": "shared"}, "key": ["/id"] },
            ]
        }))
        .unwrap();

        // The same bindings, in indirect form. Bindings `a` and `c` share an
        // entry, and an unreferenced entry is permitted.
        let indirect: response::Discovered = serde_json::from_value(json!({
            "linkedCollections": [
                { "recommendedName": "Shared", "documentSchema": {"const": "shared"}, "key": ["/id"] },
                { "recommendedName": "Unused", "documentSchema": {"const": "unused"} },
                { "recommendedName": "Other", "documentSchema": {"const": "other"}, "key": ["/id"], "isFallbackKey": true },
                { "recommendedName": "Shared!", "documentSchema": {"const": "shared"}, "key": ["/id"] },
            ],
            "bindings": [
                { "resourceConfig": {"t": "a"}, "resourcePath": ["a"], "collectionIndex": 0 },
                { "resourceConfig": {"t": "z"}, "resourcePath": ["z"], "collectionIndex": 2, "disable": true },
                { "resourceConfig": {"t": "b"}, "resourcePath": ["b"], "collectionIndex": 3 },
                { "resourceConfig": {"t": "c"}, "resourcePath": ["c"], "collectionIndex": 0 },
            ]
        }))
        .unwrap();

        // Resolving the indirect form yields the inline one.
        assert_eq!(inline_bindings(indirect.clone()).unwrap(), inline.bindings);

        // Each binding resolves to equal collection fields in either form.
        fn resolve(discovered: Discovered) -> Vec<(Binding, Collection)> {
            discovered
                .bindings
                .into_iter()
                .map(|binding| {
                    let collection = discovered.collections[binding.collection].clone();
                    (
                        Binding {
                            collection: 0,
                            ..binding
                        },
                        collection,
                    )
                })
                .collect()
        }
        let inline = resolve(parse(inline).unwrap());
        let indirect = resolve(parse(indirect).unwrap());
        assert_eq!(inline, indirect);

        // Bindings are ordered on their recommended names as sent.
        insta::assert_debug_snapshot!(
            inline.iter().map(|(b, c)| (&b.resource_path, &c.recommended_name)).collect::<Vec<_>>(),
            @r#"
        [
            (
                [
                    "z",
                ],
                "Other",
            ),
            (
                [
                    "a",
                ],
                "Shared",
            ),
            (
                [
                    "c",
                ],
                "Shared",
            ),
            (
                [
                    "b",
                ],
                "Shared",
            ),
        ]
        "#
        );
    }

    #[test]
    fn test_protocol_errors() {
        let errors = [
            // Indirect bindings may not have inline collection fields.
            json!({
                "linkedCollections": [{ "recommendedName": "one", "documentSchema": true }],
                "bindings": [{ "recommendedName": "one", "resourceConfig": {}, "collectionIndex": 0 }],
            }),
            // Indirect bindings must reference an entry.
            json!({
                "linkedCollections": [{ "recommendedName": "one", "documentSchema": true }],
                "bindings": [{ "resourceConfig": {}, "collectionIndex": 1 }],
            }),
            // Inline bindings may not have a collection index.
            json!({
                "bindings": [{ "recommendedName": "one", "documentSchema": true, "resourceConfig": {}, "collectionIndex": 1 }],
            }),
            // Recommended names must normalize to a non-empty name, inline or indirect.
            json!({
                "bindings": [{ "recommendedName": " ", "documentSchema": true, "resourceConfig": {} }],
            }),
            json!({
                "bindings": [{ "recommendedName": "!!/", "documentSchema": true, "resourceConfig": {} }],
            }),
            json!({
                "linkedCollections": [{ "documentSchema": true }],
                "bindings": [{ "resourceConfig": {}, "collectionIndex": 0 }],
            }),
            // Schemas must parse.
            json!({
                "linkedCollections": [{ "recommendedName": "one" }],
                "bindings": [{ "resourceConfig": {}, "collectionIndex": 0 }],
            }),
        ]
        .map(|fixture| serde_json::from_value::<response::Discovered>(fixture).unwrap());

        // Responses may not exceed the binding limit, in bindings or collections.
        let over = validation::MAX_BINDINGS_INDIRECT_SPECS + 1;
        let too_many_bindings = response::Discovered {
            bindings: vec![Default::default(); over],
            linked_collections: Vec::new(),
        };
        let too_many_collections = response::Discovered {
            bindings: Vec::new(),
            linked_collections: vec![Default::default(); over],
        };

        let errors = errors
            .into_iter()
            .chain([too_many_bindings, too_many_collections])
            .map(|discovered| format!("{:#}", parse(discovered).unwrap_err()))
            .collect::<Vec<_>>();

        insta::assert_debug_snapshot!(errors, @r#"
        [
            "connector protocol error: Discovered binding 0 has inline collection fields, but the response uses `linkedCollections`",
            "connector protocol error: Discovered binding 0 has `collectionIndex` 1, but the response has only 1 `linkedCollections`",
            "connector protocol error: Discovered binding 0 has a `collectionIndex`, but the response has no `linkedCollections`",
            "connector protocol error: recommended name \" \" has no characters which are valid in a collection name",
            "connector protocol error: recommended name \"!!/\" has no characters which are valid in a collection name",
            "connector protocol error: recommended name \"\" has no characters which are valid in a collection name",
            "parsing document schema of discovered collection one: EOF while parsing a value at line 1 column 0",
            "connector protocol error: Discovered response has 65437 bindings, which exceeds the limit of 65436",
            "connector protocol error: Discovered response has 65437 `linkedCollections`, which exceeds the limit of 65436",
        ]
        "#);
    }

    #[test]
    fn test_recommended_name_normalization() {
        for (name, expect) in [
            ("Foo", "Foo"),
            ("foo/bar", "foo/bar"),
            ("Faſt/Carſ", "Fast/Cars"), // First form is denormalized, assert that it gets NFKC normalized
            ("/", ""),                  // just documenting a weird edge case
            ("/foo/bar//baz/", "foo/bar_baz"), // Invalid leading, middle, & trailing slash.
            ("#੫൬    , bar-_!", "੫൬_bar-_"), // Invalid leading, middle, & trailing chars.
            ("One! two/_three", "One_two/_three"),
        ] {
            assert_eq!(
                normalize_recommended_name(name),
                expect,
                "test case: {name}"
            );
        }
    }
}
