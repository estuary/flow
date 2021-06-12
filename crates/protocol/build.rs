use std::path::Path;
use std::process::{self, Command};
use std::str;

// This section defines the attributes that we'd like to add to various types that are generated from
// the protobuf files.

static SERDE_ATTR: &str =
    "#[derive(serde::Deserialize, serde::Serialize)] #[serde(deny_unknown_fields)]";

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
    // ContentType is a JSON-encoded column of models::tables::Resources.
    TypeAttrs {
        path: "flow.ContentType",
        type_attrs: SERDE_ATTR,
        field_attrs: &[],
    },
    // EndpointType is a JSON-encoded column of models::tables::Endpoints & BuiltMaterializations.
    TypeAttrs {
        path: "flow.EndpointType",
        type_attrs: SERDE_ATTR,
        field_attrs: &[],
    },
    // TestSpec.Step.Type is a JSON-encoded column of models::tables::TestSteps.
    TypeAttrs {
        path: "flow.TestSpec.Step.Type",
        type_attrs: SERDE_ATTR,
        field_attrs: &[],
    },
    // materialize.Constraint is used in JSON-encoded fixtures of `validation` crate tests.
    TypeAttrs {
        path: "materialize.Constraint",
        type_attrs: SERDE_ATTR,
        field_attrs: &[],
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
        proto_include[3].join("go/protocols/capture/capture.proto"),
        proto_include[3].join("go/protocols/flow/flow.proto"),
        proto_include[3].join("go/protocols/materialize/materialize.proto"),
    ];

    let mut builder = prost_build::Config::new();
    builder.out_dir(Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src"));

    for attrs in TYPE_ATTRS {
        if !attrs.type_attrs.is_empty() {
            builder.type_attribute(attrs.path, attrs.type_attrs);
        }
        for &(field, field_attrs) in attrs.field_attrs {
            let path = format!("{}.{}", attrs.path, field);
            builder.field_attribute(&path, field_attrs);
        }
    }

    builder
        .compile_protos(&proto_build, &proto_include)
        .expect("failed to compile protobuf");
}
