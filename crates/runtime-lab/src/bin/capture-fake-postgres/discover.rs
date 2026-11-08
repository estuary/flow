//! Discovery of the declared tables, with schemas shaped as source-postgres
//! (sqlcapture) generates them, and the minimal schemas of SourcedSchema.

use crate::config::{ColumnType, EndpointConfig, Table};
use proto_flow::capture::response;
use serde_json::{Value, json};

pub fn discovered(config: &EndpointConfig) -> response::Discovered {
    let bindings = config
        .tables
        .iter()
        .map(|table| response::discovered::Binding {
            recommended_name: recommended_name(table),
            resource_config_json: json!({"namespace": table.namespace, "stream": table.stream})
                .to_string()
                .into(),
            document_schema_json: document_schema(table).to_string().into(),
            key: table.key.iter().map(|k| pointer(k)).collect(),
            disable: false,
            resource_path: vec![table.namespace.clone(), table.stream.clone()],
            is_fallback_key: false,
        })
        .collect();
    response::Discovered { bindings }
}

/// `<namespace>/<stream>`, lowercased, with other than `[a-z0-9-_.]` as `_`.
fn recommended_name(table: &Table) -> String {
    format!("{}/{}", table.namespace, table.stream)
        .to_lowercase()
        .chars()
        .map(|c| match c {
            'a'..='z' | '0'..='9' | '-' | '_' | '.' | '/' => c,
            _ => '_',
        })
        .collect()
}

fn pointer(field: &str) -> String {
    format!("/{}", field.replace('~', "~0").replace('/', "~1"))
}

/// Anchor of a table's column definitions, as `PublicCustomers`.
fn anchor(table: &Table) -> String {
    [&table.namespace, &table.stream]
        .iter()
        .flat_map(|part| part.split(|c: char| !c.is_ascii_alphanumeric()))
        .map(|word| {
            let mut chars = word.chars();
            match chars.next() {
                Some(first) => first.to_ascii_uppercase().to_string() + chars.as_str(),
                None => String::new(),
            }
        })
        .collect()
}

fn document_schema(table: &Table) -> Value {
    let anchor = anchor(table);
    let mut properties = serde_json::Map::new();
    for column in &table.columns {
        let type_ = ColumnType::parse(&column.type_).expect("validated");
        let mut schema = column_schema(type_, column.nullable);
        schema["description"] = json!(format!(
            "(source type: {}{})",
            if column.nullable { "" } else { "non-nullable " },
            column.type_
        ));
        properties.insert(column.name.clone(), schema);
    }

    json!({
        "$defs": {
            anchor.clone(): {
                "type": "object",
                "$anchor": anchor,
                "properties": properties,
                "required": table.key,
            },
        },
        "allOf": [
            {
                "if": {"properties": {"_meta": {"properties": {"op": {"const": "d"}}}}},
                "then": {"reduce": {"delete": true, "strategy": "merge"}},
                "else": {"reduce": {"strategy": "merge"}},
                "required": ["_meta"],
                "properties": {"_meta": meta_schema(&anchor)},
            },
            {"$ref": format!("#{anchor}")},
        ],
        "x-infer-schema": true,
    })
}

fn meta_schema(anchor: &str) -> Value {
    json!({
        "type": "object",
        "required": ["op", "source"],
        "properties": {
            "op": {
                "enum": ["c", "d", "u"],
                "description": "Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete.",
            },
            "source": {
                "type": "object",
                "required": ["schema", "table", "loc"],
                "properties": {
                    "ts_ms": {"type": "integer", "description": "Unix timestamp (in millis) at which this event was recorded by the database."},
                    "schema": {"type": "string", "description": "Database schema (namespace) of the event."},
                    "snapshot": {"type": "boolean", "description": "Snapshot is true if the record was produced from an initial table backfill and unset if produced from the replication log."},
                    "table": {"type": "string", "description": "Database table of the event."},
                    "loc": {
                        "type": "array",
                        "items": {"type": "integer"},
                        "minItems": 3,
                        "maxItems": 3,
                        "description": "Location of this WAL event as [last Commit.EndLSN; event LSN; current Begin.FinalLSN].",
                    },
                    "txid": {"type": "integer", "description": "The 32-bit transaction ID assigned by Postgres to the commit which produced this change."},
                },
            },
            "before": {
                "$ref": format!("#{anchor}"),
                "description": "Record state immediately before this change was applied.",
                "reduce": {"strategy": "firstWriteWins"},
            },
        },
        "reduce": {"strategy": "merge"},
    })
}

fn column_schema(type_: ColumnType, nullable: bool) -> Value {
    let typed = |t: &str| {
        if nullable {
            json!([t, "null"])
        } else {
            json!(t)
        }
    };
    match type_ {
        ColumnType::Bool => json!({"type": typed("boolean")}),
        ColumnType::Int2 | ColumnType::Int4 | ColumnType::Int8 => json!({"type": typed("integer")}),
        ColumnType::Float8 => {
            let mut types = vec!["number", "string"];
            if nullable {
                types.push("null");
            }
            json!({"type": types, "format": "number"})
        }
        ColumnType::Numeric => json!({"type": typed("string"), "format": "number"}),
        ColumnType::Text => json!({"type": typed("string")}),
        ColumnType::Varchar(n) => json!({"type": typed("string"), "maxLength": n}),
        ColumnType::Bytea => json!({"type": typed("string"), "contentEncoding": "base64"}),
        ColumnType::Date => json!({"type": typed("string"), "format": "date"}),
        ColumnType::Time => json!({"type": typed("string"), "format": "time"}),
        ColumnType::Timestamp | ColumnType::Timestamptz => {
            json!({"type": typed("string"), "format": "date-time"})
        }
        ColumnType::Uuid => json!({"type": typed("string"), "format": "uuid"}),
        ColumnType::Jsonb => {
            json!({"contentMediaType": "application/vnd.estuary.postgresql.jsonb+json"})
        }
    }
}

/// The schema a SourcedSchema reports for `table`: as the database describes
/// it, minimally. Every column is required, multi-type arrays drop `null`, and
/// it's closed.
pub fn sourced_schema(table: &Table) -> Value {
    let mut properties = serde_json::Map::new();
    for column in &table.columns {
        let type_ = ColumnType::parse(&column.type_).expect("validated");
        properties.insert(column.name.clone(), column_schema(type_, false));
    }
    properties.insert("_meta".to_string(), json!({"type": "object"}));

    let required: Vec<&String> = table.columns.iter().map(|c| &c.name).collect();
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
        "unevaluatedProperties": false,
    })
}

#[cfg(test)]
mod test {
    #[test]
    fn discovered_canned_tables() {
        let config: crate::config::EndpointConfig = serde_json::from_str("{}").unwrap();
        let discovered = super::discovered(&config);

        let rendered: Vec<_> = discovered
            .bindings
            .iter()
            .map(|b| {
                serde_json::json!({
                    "recommendedName": b.recommended_name,
                    "resourceConfig": serde_json::from_slice::<serde_json::Value>(&b.resource_config_json).unwrap(),
                    "resourcePath": b.resource_path,
                    "key": b.key,
                    "documentSchema": serde_json::from_slice::<serde_json::Value>(&b.document_schema_json).unwrap(),
                })
            })
            .collect();
        insta::assert_json_snapshot!(rendered);
    }

    #[test]
    fn sourced_schema_of_orders() {
        let tables = crate::config::canned_tables();
        insta::assert_json_snapshot!(super::sourced_schema(&tables[1]));
    }
}
