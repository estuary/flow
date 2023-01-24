use crate::{Convert, IntoMessages};
use connector_protocol::{
    capture::{ApplyBinding, Request, ValidateBinding},
    RawValue,
};
use proto_flow::{
    capture::{self, pull_request, validate_request},
    flow::{self, capture_spec, inference},
};

impl IntoMessages for capture::SpecRequest {
    type Message = Request;
    fn into_messages(self) -> Vec<Self::Message> {
        let Self {
            endpoint_type: _,
            endpoint_spec_json: _,
        } = self;
        vec![Request::Spec {}]
    }
}

impl IntoMessages for capture::DiscoverRequest {
    type Message = Request;
    fn into_messages(self) -> Vec<Self::Message> {
        let Self {
            endpoint_type: _,
            endpoint_spec_json,
        } = self;
        vec![Request::Discover {
            config: serde_json::from_str(&endpoint_spec_json).unwrap(),
        }]
    }
}

impl IntoMessages for capture::ValidateRequest {
    type Message = Request;
    fn into_messages(self) -> Vec<Self::Message> {
        let Self {
            capture,
            endpoint_type: _,
            endpoint_spec_json,
            bindings,
        } = self;

        vec![Request::Validate {
            name: capture,
            config: serde_json::from_str(&endpoint_spec_json).unwrap(),
            bindings: bindings.into_iter().map(Convert::convert).collect(),
        }]
    }
}

impl IntoMessages for capture::ApplyRequest {
    type Message = Request;
    fn into_messages(self) -> Vec<Self::Message> {
        let Self {
            capture,
            dry_run,
            version,
        } = self;

        let flow::CaptureSpec {
            capture,
            endpoint_type: _,
            endpoint_spec_json,
            bindings,
            interval_seconds: _,
            shard_template: _,
            recovery_log_template: _,
        } = capture.unwrap();

        vec![Request::Apply {
            name: capture,
            config: serde_json::from_str(&endpoint_spec_json).unwrap(),
            bindings: bindings.into_iter().map(Convert::convert).collect(),
            version,
            dry_run,
        }]
    }
}

impl IntoMessages for capture::PullRequest {
    type Message = Request;
    fn into_messages(self) -> Vec<Self::Message> {
        let Self { open, acknowledge } = self;
        let mut out = Vec::with_capacity(1);

        if let Some(open) = open {
            out.push(open.convert());
        }
        if let Some(capture::Acknowledge {}) = acknowledge {
            out.push(Request::Acknowledge {})
        }
        out
    }
}

impl Convert for pull_request::Open {
    type Target = Request;
    fn convert(self: Self) -> Self::Target {
        let Self {
            capture,
            driver_checkpoint_json,
            key_begin,
            key_end,
            version,
        } = self;

        let flow::CaptureSpec {
            capture,
            endpoint_type: _,
            endpoint_spec_json,
            bindings,
            interval_seconds,
            shard_template: _,
            recovery_log_template: _,
        } = capture.unwrap();

        Request::Open {
            name: capture,
            config: serde_json::from_str(&endpoint_spec_json).unwrap(),
            bindings: bindings.into_iter().map(Convert::convert).collect(),
            version,
            key_begin,
            key_end,
            driver_checkpoint: serde_json::from_slice::<RawValue>(&driver_checkpoint_json).unwrap(),
            interval_seconds,
        }
    }
}

impl Convert for validate_request::Binding {
    type Target = ValidateBinding;
    fn convert(self: Self) -> Self::Target {
        let Self {
            collection,
            resource_spec_json,
        } = self;

        Self::Target {
            collection: collection.unwrap().convert(),
            resource_config: serde_json::from_str(&resource_spec_json).unwrap(),
        }
    }
}

impl Convert for capture_spec::Binding {
    type Target = ApplyBinding;
    fn convert(self: Self) -> Self::Target {
        let Self {
            collection,
            resource_spec_json,
            resource_path,
        } = self;

        Self::Target {
            collection: collection.unwrap().convert(),
            resource_config: serde_json::from_str(&resource_spec_json).unwrap(),
            resource_path,
        }
    }
}

impl Convert for flow::CollectionSpec {
    type Target = connector_protocol::CollectionSpec;
    fn convert(self: Self) -> Self::Target {
        let Self {
            ack_json_template: _,
            collection,
            key_ptrs,
            partition_fields,
            partition_template: _,
            projections,
            read_schema_json,
            read_schema_uri: _,
            uuid_ptr: _,
            write_schema_json,
            write_schema_uri: _,
        } = self;

        let (schema, read_schema, write_schema) = if read_schema_json.is_empty() {
            (
                Some(serde_json::from_str(&write_schema_json).unwrap()),
                None,
                None,
            )
        } else {
            (
                None,
                Some(serde_json::from_str(&read_schema_json).unwrap()),
                Some(serde_json::from_str(&write_schema_json).unwrap()),
            )
        };

        Self::Target {
            name: collection,
            key: key_ptrs,
            partition_fields,
            projections: projections.into_iter().map(Convert::convert).collect(),
            schema,
            write_schema,
            read_schema,
        }
    }
}

impl Convert for flow::Projection {
    type Target = connector_protocol::Projection;
    fn convert(self: Self) -> Self::Target {
        let Self {
            ptr,
            field,
            explicit,
            is_partition_key,
            is_primary_key,
            inference,
        } = self;

        Self::Target {
            ptr,
            field,
            explicit,
            is_partition_key,
            is_primary_key,
            inference: inference.unwrap().convert(),
        }
    }
}

impl Convert for flow::Inference {
    type Target = connector_protocol::Inference;
    fn convert(self: Self) -> Self::Target {
        let Self {
            types,
            string,
            title,
            description,
            default_json,
            secret,
            exists,
        } = self;

        Self::Target {
            types,
            string: string.map(Convert::convert),
            title,
            description,
            default: if default_json.is_empty() {
                serde_json::from_str("null").unwrap()
            } else {
                serde_json::from_str(&default_json).unwrap()
            },
            secret,
            exists: inference::Exists::from_i32(exists).unwrap().convert(),
        }
    }
}

impl Convert for inference::String {
    type Target = connector_protocol::StringInference;
    fn convert(self: Self) -> Self::Target {
        let Self {
            content_encoding,
            content_type,
            format,
            is_base64,
            max_length,
        } = self;

        Self::Target {
            content_encoding,
            content_type,
            format,
            is_base64,
            max_length: max_length as usize,
        }
    }
}

impl Convert for inference::Exists {
    type Target = connector_protocol::Exists;
    fn convert(self: Self) -> Self::Target {
        match self {
            inference::Exists::Must => Self::Target::Must,
            inference::Exists::May => Self::Target::May,
            inference::Exists::Implicit => Self::Target::Implicit,
            inference::Exists::Cannot => Self::Target::Cannot,
            inference::Exists::Invalid => unreachable!("invalid exists variant"),
        }
    }
}
