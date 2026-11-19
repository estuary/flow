#!/usr/bin/env bash
# Shared helpers for build tasks. Sourced, not run.

# Format the generated Rust sources which a crate root pulls in with
# `include!()` instead of a `mod` declaration: the `*.serde.rs` bindings, and
# the `connector.rs` of proto-flow and proto-grpc. `cargo fmt` reaches a file
# only by walking `mod` declarations, so it never sees these. Everything
# checked in is formatted, so pass `--check` to verify instead of rewrite.
rustfmt_included_sources() {
    # rustfmt, unlike `cargo fmt`, has no manifest to take the edition from.
    local edition
    edition="$(sed -n 's/^edition = "\(.*\)"$/\1/p' Cargo.toml)"

    local src files
    for src in crates/proto-flow/src crates/proto-gazette/src crates/proto-grpc/src; do
        # Every `include!()` is in lib.rs, so its path is relative to $src.
        mapfile -t files < <(sed -n "s|.*include!(\"\([^\"]*\)\").*|$src/\1|p" "$src/lib.rs")

        # rustfmt reads stdin when given no files.
        if [ "${#files[@]}" -gt 0 ]; then
            rustfmt --edition "$edition" "$@" "${files[@]}"
        fi
    done
}
