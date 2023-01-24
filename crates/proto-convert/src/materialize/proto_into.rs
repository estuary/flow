use crate::{Convert, IntoMessages};
use connector_protocol::{
    materialize::{ApplyBinding, FieldSelection, Request, ValidateBinding},
    RawValue,
};
use proto_flow::{
    flow::{self, materialization_spec},
    materialize::{self, transaction_request, validate_request},
};
use serde_json::Value;
use tuple::{Element, TupleUnpack};

impl IntoMessages for materialize::SpecRequest {
    type Message = Request;
    fn into_messages(self) -> Vec<Self::Message> {
        let Self {
            endpoint_type: _,
            endpoint_spec_json: _,
        } = self;
        vec![Request::Spec {}]
    }
}

impl IntoMessages for materialize::ValidateRequest {
    type Message = Request;
    fn into_messages(self) -> Vec<Self::Message> {
        let Self {
            materialization,
            endpoint_type: _,
            endpoint_spec_json,
            bindings,
        } = self;

        vec![Request::Validate {
            name: materialization,
            config: serde_json::from_str(&endpoint_spec_json).unwrap(),
            bindings: bindings.into_iter().map(Convert::convert).collect(),
        }]
    }
}

impl IntoMessages for materialize::ApplyRequest {
    type Message = Request;
    fn into_messages(self) -> Vec<Self::Message> {
        let Self {
            materialization,
            dry_run,
            version,
        } = self;

        let flow::MaterializationSpec {
            materialization,
            endpoint_type: _,
            endpoint_spec_json,
            bindings,
            shard_template: _,
            recovery_log_template: _,
        } = materialization.unwrap();

        vec![Request::Apply {
            name: materialization,
            config: serde_json::from_str(&endpoint_spec_json).unwrap(),
            bindings: bindings.into_iter().map(Convert::convert).collect(),
            version,
            dry_run,
        }]
    }
}

impl IntoMessages for materialize::TransactionRequest {
    type Message = Request;
    fn into_messages(self) -> Vec<Self::Message> {
        let mut out = Vec::with_capacity(1);

        let Self {
            open,
            acknowledge,
            load,
            flush,
            store,
            start_commit,
        } = self;

        if let Some(open) = open {
            out.push(open.convert());
        }
        if let Some(transaction_request::Acknowledge {}) = acknowledge {
            out.push(Request::Acknowledge {});
        }
        if let Some(transaction_request::Load {
            binding,
            arena,
            packed_keys,
        }) = load
        {
            for flow::Slice { begin, end } in packed_keys {
                let key_packed = &arena[begin as usize..end as usize];
                let key: Vec<Element> = Vec::unpack_root(key_packed).unwrap();
                let key: Vec<Value> = key.into_iter().map(element_to_value).collect();

                out.push(Request::Load {
                    binding,
                    key_packed: hex::encode(key_packed),
                    key,
                });
            }
        }
        if let Some(transaction_request::Flush {}) = flush {
            out.push(Request::Flush {});
        }
        if let Some(transaction_request::Store {
            binding,
            arena,
            packed_keys,
            packed_values,
            docs_json,
            exists,
        }) = store
        {
            for i in 0..packed_keys.len() {
                let flow::Slice { begin, end } = packed_keys[i];
                let key_packed = &arena[begin as usize..end as usize];
                let key: Vec<Element> = Vec::unpack_root(key_packed).unwrap();
                let key: Vec<Value> = key.into_iter().map(element_to_value).collect();

                let flow::Slice { begin, end } = packed_values[i];
                let values_packed = &arena[begin as usize..end as usize];
                let values: Vec<Element> = Vec::unpack_root(values_packed).unwrap();
                let values: Vec<Value> = values.into_iter().map(element_to_value).collect();

                let flow::Slice { begin, end } = docs_json[i];
                let doc_json = &arena[begin as usize..end as usize];
                let doc = serde_json::from_slice(doc_json).unwrap();

                out.push(Request::Store {
                    binding,
                    key_packed: hex::encode(key_packed),
                    key,
                    values,
                    doc,
                    exists: exists[i],
                });
            }
        }
        if let Some(transaction_request::StartCommit { runtime_checkpoint }) = start_commit {
            out.push(Request::StartCommit {
                runtime_checkpoint: base64::encode(runtime_checkpoint),
            });
        }

        out
    }
}

impl Convert for transaction_request::Open {
    type Target = Request;
    fn convert(self: Self) -> Self::Target {
        let Self {
            materialization,
            driver_checkpoint_json,
            key_begin,
            key_end,
            version,
        } = self;

        let flow::MaterializationSpec {
            materialization,
            endpoint_type: _,
            endpoint_spec_json,
            bindings,
            shard_template: _,
            recovery_log_template: _,
        } = materialization.unwrap();

        Request::Open {
            name: materialization,
            config: serde_json::from_str(&endpoint_spec_json).unwrap(),
            bindings: bindings.into_iter().map(Convert::convert).collect(),
            version,
            key_begin,
            key_end,
            driver_checkpoint: serde_json::from_slice::<RawValue>(&driver_checkpoint_json).unwrap(),
        }
    }
}

impl Convert for validate_request::Binding {
    type Target = ValidateBinding;
    fn convert(self: Self) -> Self::Target {
        let Self {
            collection,
            resource_spec_json,
            field_config_json,
        } = self;

        ValidateBinding {
            collection: collection.unwrap().convert(),
            resource_config: serde_json::from_str(&resource_spec_json).unwrap(),
            field_config: field_config_json
                .into_iter()
                .map(|(k, v)| (k, serde_json::from_str(&v).unwrap()))
                .collect(),
        }
    }
}

impl Convert for materialization_spec::Binding {
    type Target = ApplyBinding;

    fn convert(self: Self) -> Self::Target {
        let Self {
            collection,
            resource_spec_json,
            resource_path,
            delta_updates,
            field_selection,
            shuffle: _,
        } = self;

        ApplyBinding {
            collection: collection.unwrap().convert(),
            resource_config: serde_json::from_str(&resource_spec_json).unwrap(),
            resource_path,
            delta_updates,
            field_selection: field_selection.unwrap().convert(),
        }
    }
}

impl Convert for flow::FieldSelection {
    type Target = FieldSelection;
    fn convert(self: Self) -> Self::Target {
        let Self {
            keys,
            document,
            field_config_json,
            values,
        } = self;

        let field_config = field_config_json
            .into_iter()
            .map(|(field, config)| (field, serde_json::from_str(&config).unwrap()))
            .collect();

        FieldSelection {
            keys,
            values,
            document: if document.is_empty() {
                None
            } else {
                Some(document)
            },
            field_config,
        }
    }
}

fn element_to_value(element: tuple::Element) -> Value {
    match element {
        Element::Bool(b) => Value::Bool(b),
        Element::Bytes(buf) => serde_json::from_slice::<Value>(&buf).unwrap(),
        Element::Double(d) => Value::Number(serde_json::Number::from_f64(d).unwrap()),
        Element::Float(f) => Value::Number(serde_json::Number::from_f64(f as f64).unwrap()),
        Element::Int(i) => Value::Number(i.into()),
        Element::Nil => Value::Null,
        Element::String(s) => Value::String(s.to_string()),
        elem => panic!("tuple element {:?} not supported", elem),
    }
}
