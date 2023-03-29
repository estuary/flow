pub use std::path::{Path, PathBuf};
pub use std::process;

pub struct Boilerplate {
    // Canonical path to this repository's root.
    pub repo_root: PathBuf,
    // Canonical path to our vendored Gazette and gogoproto protobufs.
    pub vendor_root: PathBuf,
    // The './src' directory of the current crate.
    pub src_dir: PathBuf,
    // The protobuf descriptor path.
    pub descriptor_path: PathBuf,
}

impl Boilerplate {
    pub fn create() -> Self {
        let repo_root = std::fs::canonicalize(
            std::env::current_dir()
                .expect("resolving current dir")
                .join("../../"),
        )
        .expect("canonical repo root path");
        let vendor_root = repo_root.join("crates/proto-build/vendor");
        let src_dir = Path::new(&std::env::var("CARGO_MANIFEST_DIR").unwrap()).join("src");

        // NOTE(johnny): We used to resolve protobuf dependencies through go.mod and `go list`.
        // That introduced an unfortunate coupling on having the entire Go toolchain installed,
        // so we've switched to vendoring these dependencies.
        /*
        let resolve = |module| {
            if !process::Command::new("go")
                .args(&["mod", "download", module])
                .status()
                .expect("failed to run 'go'")
                .success()
            {
                panic!("go mod download failed");
            }

            let go_list = process::Command::new("go")
                .args(&["list", "-f", "{{ .Dir }}", "-m", module])
                .stderr(process::Stdio::inherit())
                .output()
                .expect("failed to run 'go'");

            if !go_list.status.success() {
                panic!("go list {} failed", module);
            }

            Path::new(std::str::from_utf8(&go_list.stdout).unwrap().trim_end()).to_owned()
        };

        let gazette_root = resolve("go.gazette.dev/core");
        let protobuf_root = resolve("github.com/gogo/protobuf");
        */

        // Descriptors written by prost, and read by pbjson.
        let descriptor_path =
            Path::new(&std::env::var("OUT_DIR").unwrap()).join("proto_descriptor.bin");

        Self {
            repo_root,
            vendor_root,
            src_dir,
            descriptor_path,
        }
    }

    pub fn skip() -> bool {
        std::env::var("SKIP_PROTO_BUILD").is_ok()
    }

    pub fn proto_include(&self) -> Vec<&Path> {
        // The vendor directory must appear first, or `protoc` gets confused
        // about also finding it in the repository root.
        vec![&self.vendor_root, &self.repo_root]
    }

    pub fn resolve_gazette_targets(&self) -> Vec<PathBuf> {
        let targets = vec![
            self.vendor_root.join("broker/protocol/protocol.proto"),
            self.vendor_root.join("consumer/protocol/protocol.proto"),
            self.vendor_root
                .join("consumer/recoverylog/recorded_op.proto"),
        ];
        Self::rerun_if_changed(&targets);
        targets
    }

    pub fn resolve_flow_targets(&self) -> Vec<PathBuf> {
        let targets = vec![
            self.repo_root.join("go/protocols/capture/capture.proto"),
            self.repo_root.join("go/protocols/derive/derive.proto"),
            self.repo_root.join("go/protocols/flow/flow.proto"),
            self.repo_root
                .join("go/protocols/materialize/materialize.proto"),
            self.repo_root.join("go/protocols/ops/ops.proto"),
            self.repo_root.join("go/protocols/runtime/runtime.proto"),
        ];
        Self::rerun_if_changed(&targets);
        targets
    }

    fn rerun_if_changed(targets: &[PathBuf]) {
        for path in targets.iter() {
            println!("cargo:rerun-if-changed={}", path.display());
        }
        // Emitting explicit paths drops the default behavior of watching
        // for changes to files in the crate root, which we also want:
        // See https://doc.rust-lang.org/cargo/reference/build-scripts.html#rerun-if-changed
        println!("cargo:rerun-if-changed=.");
    }
}
