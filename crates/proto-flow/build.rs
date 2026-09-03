// The `Request` and `Response` messages of the connector protocols are
// semantically an XOR over their sub-message fields, but they're declared as
// plain optional fields so that Go's generated code keeps flat accessors.
// Rust pays dearly for that: prost embeds each sub-message by value, so a
// `capture::Request` is as large as the sum of every variant and the runtime
// spends real memory bandwidth moving them.
//
// So, before prost and pbjson see the descriptors, we rewrite them to gather
// those fields into a `oneof kind`. The wire and JSON encodings of a oneof are
// identical to the equivalent optional fields, so this is purely a Rust
// binding change: nothing else in the platform observes it.
//
// The `.proto` files point here rather than repeating any of this, and
// README.md carries the fuller write-up. When a `.proto` gains a new
// sub-message field of one of these messages, add it to the table below: the
// guard in `inject_oneofs` fails the build if you don't.
#[cfg(feature = "generate")]
const ONEOFS: &[(&str, &str, &[&str])] = &[
    (
        ".capture.Request",
        "kind",
        &[
            "spec",
            "discover",
            "validate",
            "apply",
            "open",
            "acknowledge",
        ],
    ),
    (
        ".capture.Response",
        "kind",
        &[
            "spec",
            "discovered",
            "validated",
            "applied",
            "opened",
            "captured",
            "sourced_schema",
            "checkpoint",
            "backfill_begin",
            "backfill_complete",
        ],
    ),
    (
        ".derive.Request",
        "kind",
        &[
            "spec",
            "validate",
            "open",
            "read",
            "flush",
            "start_commit",
            "reset",
        ],
    ),
    (
        ".derive.Response",
        "kind",
        &[
            "spec",
            "validated",
            "opened",
            "published",
            "flushed",
            "started_commit",
        ],
    ),
    (
        ".materialize.Request",
        "kind",
        &[
            "spec",
            "validate",
            "apply",
            "open",
            "load",
            "flush",
            "store",
            "start_commit",
            "acknowledge",
        ],
    ),
    (
        ".materialize.Response",
        "kind",
        &[
            "spec",
            "validated",
            "applied",
            "opened",
            "loaded",
            "flushed",
            "started_commit",
            "acknowledged",
        ],
    ),
];

// Doc comment attached to each injected oneof, surfaced on the generated
// `kind` field and `Kind` enum.
#[cfg(feature = "generate")]
const ONEOF_DOC: &str = concat!(
    " The mutually-exclusive sub-messages of this message.\n",
    "\n",
    " This `oneof` is not declared in the `.proto` file: it's injected into the\n",
    " descriptor set by `crates/proto-flow/build.rs`, and exists only in the Rust\n",
    " bindings. Its wire and JSON encodings are those of the declared optional\n",
    " fields.\n",
);

// Gather the sub-message fields of each ONEOFS entry into an injected `oneof`.
//
// Panics if the table has drifted from the `.proto` files, which is the point:
// a sub-message field that's added to one of these messages and not listed
// would otherwise silently remain outside the enum, defeating both the XOR
// invariant and the size reduction.
#[cfg(feature = "generate")]
fn inject_oneofs(fds: &mut prost_types::FileDescriptorSet) {
    use prost_types::field_descriptor_proto::{Label, Type};

    for &(fq_message, oneof_name, variants) in ONEOFS {
        let (package, message_name) = fq_message
            .strip_prefix('.')
            .expect("fully-qualified name")
            .rsplit_once('.')
            .expect("package-qualified message name");

        let (file, message_index) = fds
            .file
            .iter_mut()
            .filter(|file| file.package() == package)
            .find_map(|file| {
                file.message_type
                    .iter()
                    .position(|message| message.name() == message_name)
                    .map(|index| (file, index))
            })
            .unwrap_or_else(|| panic!("{fq_message} is not in the descriptor set"));

        let message = &mut file.message_type[message_index];

        assert!(
            message.oneof_decl.is_empty(),
            "{fq_message} already declares a oneof, which injection would re-index"
        );
        let oneof_index = 0;

        message.oneof_decl.push(prost_types::OneofDescriptorProto {
            name: Some(oneof_name.to_string()),
            options: None,
        });

        for field in message.field.iter_mut() {
            // `internal` is a sibling of the oneof, not a variant of it,
            // and is the only sub-message field allowed to be unlisted.
            let is_sub_message =
                field.r#type() == Type::Message && field.label() != Label::Repeated;

            if !variants.contains(&field.name()) {
                assert!(
                    !is_sub_message || field.name() == "internal",
                    "{fq_message}.{} is a sub-message field which is not a listed variant: add it to ONEOFS in crates/proto-flow/build.rs",
                    field.name(),
                );
                continue;
            }
            assert!(
                is_sub_message,
                "{fq_message}.{} is listed as a oneof variant but is not a singular message field",
                field.name(),
            );
            field.oneof_index = Some(oneof_index);
        }

        for variant in variants {
            assert!(
                message
                    .field
                    .iter()
                    .any(|field| field.name() == *variant && field.oneof_index.is_some()),
                "{fq_message} has no field {variant}: remove it from ONEOFS in crates/proto-flow/build.rs",
            );
        }

        // prost-build unconditionally looks up the SourceCodeInfo location of
        // every oneof it generates, and panics if there isn't one. Synthesize
        // it; its leading comment becomes the generated doc comment.
        let source_info = file
            .source_code_info
            .as_mut()
            .expect("descriptors include source info");

        source_info
            .location
            .push(prost_types::source_code_info::Location {
                // Path of a oneof_decl: message_type[i].oneof_decl[j].
                path: vec![4, message_index as i32, 8, oneof_index],
                leading_comments: Some(ONEOF_DOC.to_string()),
                ..Default::default()
            });
    }
}

// pbjson serializes a oneof only through its parent message: it matches the
// enum inline and emits the set variant as a flattened field. Nothing
// implements `Serialize` for the `Kind` enum itself, which callers want when
// reporting a mis-matched variant without hauling the whole message along.
// Generate that impl for each ONEOFS entry, emitting exactly the field pbjson
// would, so a bare `Kind` serializes as its parent does with an empty
// `internal`. There is deliberately no `Deserialize`: the parent message is
// the only decoding entry point.
//
// Returns `(serde file, generated code)` pairs to append to pbjson's output.
#[cfg(feature = "generate")]
fn oneof_serialize_impls(fds: &prost_types::FileDescriptorSet) -> Vec<(String, String)> {
    // prost-build's ident helpers are private. This is enough for the
    // snake_case field names in ONEOFS, which the assert below pins.
    fn upper_camel(s: &str) -> String {
        s.split('_')
            .map(|part| {
                let mut chars = part.chars();
                match chars.next() {
                    Some(c) => c.to_uppercase().chain(chars).collect::<String>(),
                    None => String::new(),
                }
            })
            .collect()
    }

    ONEOFS
        .iter()
        .map(|&(fq_message, oneof_name, _)| {
            let (package, message_name) = fq_message
                .strip_prefix('.')
                .expect("fully-qualified name")
                .rsplit_once('.')
                .expect("package-qualified message name");

            let message = fds
                .file
                .iter()
                .filter(|file| file.package() == package)
                .find_map(|file| {
                    file.message_type
                        .iter()
                        .find(|message| message.name() == message_name)
                })
                .expect("located by inject_oneofs");

            // The generated module of a top-level message is its snake_case
            // name; ONEOFS only holds single-word messages so lowercase suffices.
            assert!(
                message_name.chars().filter(|c| c.is_uppercase()).count() == 1,
                "{fq_message}: extend the module-name derivation for multi-word messages"
            );
            let module = message_name.to_lowercase();
            let enum_name = upper_camel(oneof_name);

            let arms = message
                .field
                .iter()
                .filter(|field| field.oneof_index.is_some())
                .map(|field| {
                    assert!(
                        field.name().chars().all(|c| c.is_ascii_lowercase() || c == '_'),
                        "{fq_message}.{}: extend upper_camel for this field name",
                        field.name(),
                    );
                    format!(
                        "            {module}::{enum_name}::{variant}(v) => {{\n                struct_ser.serialize_field(\"{json_name}\", v)?;\n            }}\n",
                        variant = upper_camel(field.name()),
                        json_name = field.json_name(),
                    )
                })
                .collect::<String>();

            let code = format!(
                r#"
impl serde::Serialize for {module}::{enum_name} {{
    #[allow(deprecated)]
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {{
        use serde::ser::SerializeStruct;
        let mut struct_ser = serializer.serialize_struct("{package}.{message_name}", 1)?;
        match self {{
{arms}        }}
        struct_ser.end()
    }}
}}
"#
            );

            (format!("./{package}.serde.rs"), code)
        })
        .collect()
}

#[cfg(feature = "generate")]
fn main() {
    let b = proto_build::Boilerplate::create();
    let proto_build = b.resolve_flow_targets();

    let mut config = prost_build::Config::new();
    config
        .out_dir(&b.src_dir)
        .btree_map(&["."]) // Make ordering stable for snapshots.
        .bytes(&["."]) // Represent byte fields with Bytes, not Vec<u8>.
        .file_descriptor_set_path(&b.descriptor_path)
        .compile_well_known_types()
        .extern_path(".consumer", "::proto_gazette::consumer")
        .extern_path(".google.protobuf", "::pbjson_types")
        .extern_path(".protocol", "::proto_gazette::broker")
        .extern_path(".recoverylog", "::proto_gazette::recoverylog");

    // A Rust enum is as large as its largest variant, so gathering the
    // connector sub-messages into a `oneof` only helps if the startup-time
    // variants -- which are large, exchanged once, and never on a hot path --
    // are behind a pointer. Box them, and leave the hot-path variants inline
    // so that steady-state document flow costs no allocation.
    //
    // These paths address a oneof variant as `<message>.<oneof>.<field>`.
    for path in [
        ".capture.Request.kind.discover",
        ".capture.Request.kind.validate",
        ".capture.Request.kind.apply",
        ".capture.Request.kind.open",
        ".capture.Response.kind.spec",
        ".derive.Request.kind.validate",
        ".derive.Request.kind.open",
        ".derive.Response.kind.spec",
        ".materialize.Request.kind.validate",
        ".materialize.Request.kind.apply",
        ".materialize.Request.kind.open",
        ".materialize.Response.kind.spec",
        ".connector.Response.Started.spec.capture",
        ".connector.Response.Started.spec.derive",
        ".connector.Response.Started.spec.materialize",
    ] {
        config.boxed(path);
    }

    // Inlined `CollectionSpec` copies, which are large and are being replaced
    // by the `collection_index` encoding (see `src/linked.rs`). Boxing them
    // sizes a binding or transform to its own fields rather than to a
    // collection it usually shares with its siblings.
    for path in [
        ".flow.CollectionSpec.derivation",
        ".flow.CaptureSpec.Binding.collection",
        ".flow.MaterializationSpec.Binding.collection",
        ".flow.CollectionSpec.Derivation.Transform.collection",
        ".capture.Request.Validate.Binding.collection",
        ".materialize.Request.Validate.Binding.collection",
        ".derive.Request.Validate.Transform.collection",
    ] {
        config.boxed(path);
    }

    let mut fds = config
        .load_fds(&proto_build, &b.proto_include())
        .expect("failed to load protobuf descriptors");

    inject_oneofs(&mut fds);
    let oneof_serialize_impls = oneof_serialize_impls(&fds);

    // `load_fds` wrote protoc's un-rewritten output to `descriptor_path`, which
    // pbjson reads below. Overwrite it so pbjson sees the injected oneofs too.
    std::fs::write(&b.descriptor_path, prost::Message::encode_to_vec(&fds))
        .expect("write rewritten descriptors");

    config.compile_fds(fds).expect("failed to compile protobuf");

    pbjson_build::Builder::new()
        .out_dir(&b.src_dir)
        .register_descriptors(&std::fs::read(b.descriptor_path).expect("read descriptors"))
        .unwrap()
        .btree_map(["."]) // Make ordering stable for snapshots.
        .ignore_unknown_fields()
        .build(&[
            ".flow",
            ".capture",
            ".connector",
            ".derive",
            ".materialize",
            ".ops",
            ".runtime",
            ".shuffle",
        ])
        .expect("building pbjson");

    // Next, apply some fixups to the serde implementations generated by pbjson:
    // * Fields ending in "_json" are borrowed and serialized as &RawValue,
    //   and are deserialized into a Box<RawValue> that's converted to byte::Bytes.
    // * Fields ending in "_json_map" are mapped from BTreeMap<String, byte::Bytes>
    //   to BTreeMap<&str, &RawValue>, and deserialize in reverse.
    // * Fields ending in "_json_vec" are mapped from Vec<byte::Bytes> to Vec<&RawValue>,
    //   and deserialize in reverse as well.
    // * Our stats documents' bytesTotal, docsTotal, and bytesBehind fields are typed as u64 to allow for
    //   tallying relatively large values in a single document but we do not want
    //   this value serialized as a string, so we remove the string conversion.
    // * A oneof variant which is explicitly `null` is treated as unset, rather
    //   than as an occupied variant which conflicts with its siblings.

    let ser_json_re =
        regex::Regex::new(r#"struct_ser\.serialize_field\((".+"), pbjson::private::base64::encode\(&(self\..*_json)\)\.as_str\(\)\)\?"#).unwrap();
    let ser_json_map_re = regex::Regex::new(
        r#"let v: std::collections::HashMap<_, _> = (self\..*_json_map)\.iter\(\)\s*\.map\(\|\(k, v\)\| \(k, pbjson::private::base64::encode\(v\)\)\)\.collect\(\);\s*struct_ser\.serialize_field\((".+"), &v\)\?"#
    ).unwrap();
    let ser_json_vec_re =
        regex::Regex::new(r#"struct_ser\.serialize_field\((".+"), &(self\..*_json_vec)\.iter\(\).map\(pbjson::private::base64::encode\).collect::<Vec<_>>\(\)\)\?"#)
            .unwrap();
    let ser_int64_re =
        regex::Regex::new(r#"struct_ser\.serialize_field\("(bytesTotal|docsTotal|bytesBehind)", ToString::to_string\(&self\.(bytes_total|docs_total|bytes_behind)\).as_str\(\)\)\?;"#)
            .unwrap();

    // pbjson rejects a oneof variant key whose sibling is already set, before
    // it looks at the value. Proto3 JSON says an explicit `null` means "unset",
    // and connectors which model the message as a struct of optional fields do
    // send `{"opened":{},"published":null}`. Parse the value first and ignore a
    // null, so that only two *occupied* variants are a duplicate.
    let de_oneof_re = regex::Regex::new(
        r#"(?m)^([ ]*)if (\w+__)\.is_some\(\) \{\s*return Err\(serde::de::Error::duplicate_field\((".+?")\)\);\s*\}\s*(\w+__) = map_\.next_value::<::std::option::Option<_>>\(\)\?\.map\(([\w:]+)\)\s*;"#,
    )
    .unwrap();

    let de_json_re =
        regex::Regex::new(r#"_json__ =[^;]+(::pbjson::private::BytesDeserialize<_>)>"#).unwrap();
    let de_json_map_re =
        regex::Regex::new(r#"_json_map__ =[^;]+(::pbjson::private::BytesDeserialize<_>)>"#)
            .unwrap();
    let de_json_vec_re =
        regex::Regex::new(r#"_json_vec__ =[^;]+(::pbjson::private::BytesDeserialize<_>)>"#)
            .unwrap();

    for path in [
        "./capture.serde.rs",
        "./connector.serde.rs",
        "./derive.serde.rs",
        "./flow.serde.rs",
        "./materialize.serde.rs",
        "./ops.serde.rs",
        "./runtime.serde.rs",
    ] {
        let root = &b.src_dir;
        let mut buf = std::fs::read_to_string(&root.join(path)).unwrap();

        // Handle _json fields.
        while let Some(capture) = ser_json_re.captures(&buf) {
            let range = capture.get(0).unwrap().range();
            let field = &buf[capture.get(2).unwrap().range()];
            let name = &buf[capture.get(1).unwrap().range()];
            buf.replace_range(
                range,
                &format!("struct_ser.serialize_field({name}, &crate::as_raw_json(&{field})?)?"),
            );
        }
        while let Some(capture) = de_json_re.captures(&buf) {
            let range = capture.get(1).unwrap().range();
            buf.replace_range(range, &format!("crate::RawJSONDeserialize"));
        }

        // Handle _json_map fields.
        while let Some(capture) = ser_json_map_re.captures(&buf) {
            let range = capture.get(0).unwrap().range();
            let field = &buf[capture.get(1).unwrap().range()];
            let name = &buf[capture.get(2).unwrap().range()];
            buf.replace_range(
                range,
                &format!("struct_ser.serialize_field({name}, &crate::as_raw_json_map(&{field})?)?"),
            );
        }
        while let Some(capture) = de_json_map_re.captures(&buf) {
            let range = capture.get(1).unwrap().range();
            buf.replace_range(range, &format!("crate::RawJSONDeserialize"));
        }

        // Handle _json_vec fields.
        while let Some(capture) = ser_json_vec_re.captures(&buf) {
            let range = capture.get(0).unwrap().range();
            let field = &buf[capture.get(2).unwrap().range()];
            let name = &buf[capture.get(1).unwrap().range()];
            buf.replace_range(
                range,
                &format!("struct_ser.serialize_field({name}, &crate::as_raw_json_vec(&{field})?)?"),
            );
        }
        while let Some(capture) = de_json_vec_re.captures(&buf) {
            let range = capture.get(1).unwrap().range();
            buf.replace_range(range, &format!("crate::RawJSONDeserialize"));
        }

        // Handle serializing stats counters as integers rather than quoted integers.
        while let Some(capture) = ser_int64_re.captures(&buf) {
            let range = capture.get(0).unwrap().range();
            buf.replace_range(
                range,
                &format!(
                    r#"struct_ser.serialize_field("{}", &self.{})?;"#,
                    capture.get(1).unwrap().as_str(),
                    capture.get(2).unwrap().as_str(),
                ),
            );
        }

        // Handle oneof variants which are explicitly null. These files are
        // generator output which `cargo fmt` doesn't visit, so the replacement
        // carries the indentation of the arm it replaces.
        let mut cursor = 0;
        while let Some(capture) = de_oneof_re.captures_at(&buf, cursor) {
            let range = capture.get(0).unwrap().range();
            let indent = buf[capture.get(1).unwrap().range()].to_string();
            let oneof = buf[capture.get(2).unwrap().range()].to_string();
            let name = buf[capture.get(3).unwrap().range()].to_string();
            let variant = buf[capture.get(5).unwrap().range()].to_string();

            // The `regex` crate has no backreferences, so the guard and the
            // assignment are matched independently: pin that they agree.
            assert_eq!(oneof, buf[capture.get(4).unwrap().range()]);

            let replacement = format!(
                "{indent}if let Some(v) = map_.next_value::<::std::option::Option<_>>()? {{\n\
                 {indent}    if {oneof}.is_some() {{\n\
                 {indent}        return Err(serde::de::Error::duplicate_field({name}));\n\
                 {indent}    }}\n\
                 {indent}    {oneof} = Some({variant}(v));\n\
                 {indent}}}"
            );
            cursor = range.start + replacement.len();
            buf.replace_range(range, &replacement);
        }

        for (_, code) in oneof_serialize_impls.iter().filter(|(p, _)| p == path) {
            buf.push_str(code);
        }

        std::fs::write(&root.join(path), buf).unwrap();
    }
}

#[cfg(not(feature = "generate"))]
fn main() {}
