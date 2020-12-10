use std::path::Path;
use std::process::{self, Command};
use std::str;
use tonic_build;

// This section defines the attributes that we'd like to add to various types that are generated from
// the protobuf files.

static SERDE_ATTR: &str =
    "#[derive(serde::Deserialize, serde::Serialize)] #[serde(deny_unknown_fields)]";

// This is a hack to allow our prost Message types impl De/Serialize. We ought to be able to
// remove this once: https://github.com/danburkert/prost/issues/277 is merged, as that will
// allow us to use something like this: https://github.com/fdeantoni/prost-wkt to make the
// "well known types" implement De/Serialize.
static DURATION_ATTR: &str =
    "#[serde(deserialize_with = \"crate::deserialize_duration\", serialize_with = \"crate::serialize_duration\")]";

static OPTIONAL_STRING_ATTR: &str = r##"#[serde(default, skip_serializing_if = "str::is_empty")]
    #[doc("This field is optional. An empty String denotes a missing value.")]"##;
static OPTIONAL_VEC_ATTR: &str = r##"#[serde(default, skip_serializing_if = "Vec::is_empty")]
    #[doc("This field is optional. An empty Vec represents a missing value.")]"##;
static OPTIONAL_STRUCT_ATTR: &str =
    r##"#[serde(default, skip_serializing_if = "Option::is_none")]"##;
static OPTIONAL_U32_ATTR: &str = r##"#[serde(default, skip_serializing_if = "crate::u32_is_0")]
    #[doc("This field is optional. A value of 0 represents a missing value.")]"##;

#[derive(Copy, Clone, Debug)]
struct TypeAttrs<'a> {
    path: &'a str,
    type_attrs: &'a str,
    field_attrs: &'a [(&'a str, &'a str)],
}

/// This is where we configure the attributes that will be added to each protobuf generated type.
/// The `path` matches based on the rules documented here:
/// https://docs.rs/prost-build/0.6.1/prost_build/struct.Config.html#arguments
/// `field_attrs` holds tuples of field name to field attributes.
static TYPE_ATTRS: &'static [TypeAttrs<'static>] = &[
    TypeAttrs {
        path: "protocol.Label",
        type_attrs: SERDE_ATTR,
        field_attrs: &[],
    },
    TypeAttrs {
        path: "protocol.LabelSet",
        type_attrs: SERDE_ATTR,
        field_attrs: &[],
    },
    TypeAttrs {
        path: "protocol.JournalSpec",
        type_attrs: SERDE_ATTR,
        field_attrs: &[],
    },
    TypeAttrs {
        path: "protocol.JournalSpec.Fragment",
        type_attrs: SERDE_ATTR,
        field_attrs: &[
            ("flush_interval", DURATION_ATTR),
            ("refresh_interval", DURATION_ATTR),
            ("retention", DURATION_ATTR),
        ],
    },
    TypeAttrs {
        path: "flow.CollectionSpec",
        type_attrs: SERDE_ATTR,
        field_attrs: &[
            ("uuid_ptr", OPTIONAL_STRING_ATTR),
            ("partition_fields", OPTIONAL_VEC_ATTR),
            ("ack_json_template", OPTIONAL_VEC_ATTR),
            ("journal_spec", OPTIONAL_STRUCT_ATTR),
        ],
    },
    TypeAttrs {
        path: "flow.Projection",
        type_attrs: SERDE_ATTR,
        field_attrs: &[("inference", OPTIONAL_STRUCT_ATTR)],
    },
    TypeAttrs {
        path: "flow.Inference",
        type_attrs: SERDE_ATTR,
        field_attrs: &[
            ("string", OPTIONAL_STRUCT_ATTR),
            ("title", OPTIONAL_STRING_ATTR),
            ("description", OPTIONAL_STRING_ATTR),
        ],
    },
    TypeAttrs {
        path: "flow.Inference.String",
        type_attrs: SERDE_ATTR,
        field_attrs: &[
            ("content_type", OPTIONAL_STRING_ATTR),
            ("format", OPTIONAL_STRING_ATTR),
            ("max_length", OPTIONAL_U32_ATTR),
        ],
    },
];

fn main() {
    let mut proto_include = Vec::new();

    let go_modules = &[
        "go.gazette.dev/core",
        "github.com/gogo/protobuf",
        "github.com/golang/protobuf", // Remove?
        "github.com/estuary/flow",
    ];
    for module in go_modules {
        let go_list = Command::new("go")
            .args(&["list", "-f", "{{ .Dir }}", "-m", module])
            .stderr(process::Stdio::inherit())
            .output()
            .expect("failed to run 'go'");

        if !go_list.status.success() {
            panic!("go list go.gazette.dev/core failed");
        }

        let dir = str::from_utf8(&go_list.stdout).unwrap().trim_end();
        proto_include.push(Path::new(dir).to_owned());
    }

    println!("proto_include: {:?}", proto_include);

    let proto_build = [
        proto_include[0].join("broker/protocol/protocol.proto"),
        proto_include[0].join("consumer/protocol/protocol.proto"),
        proto_include[0].join("consumer/recoverylog/recorded_op.proto"),
        proto_include[3].join("go/protocols/flow/flow.proto"),
    ];

    let mut builder = tonic_build::configure()
        .build_server(true)
        .build_client(true)
        .out_dir(Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src"));

    for attrs in TYPE_ATTRS {
        if !attrs.type_attrs.is_empty() {
            builder = builder.type_attribute(attrs.path, attrs.type_attrs);
        }
        for &(field, field_attrs) in attrs.field_attrs {
            let path = format!("{}.{}", attrs.path, field);
            builder = builder.field_attribute(&path, field_attrs);
        }
    }

    builder
        .compile(&proto_build, &proto_include)
        .expect("failed to compile protobuf");
}
