# Flow all up in your browser

This is the source for the `flow-web` NPM package, which exposes Javascript/Typescript bindings to Flow library functions in Web Assembly (WASM). The gist is
that we compile this Rust crate to WASM and then generate the corresponding JS/TS files using `wasm-bindgen`. We use `wasm-pack` to put everything together into
an NPM package that works with Webpack, and publish that to Github packages.

### Prerequisites

In order to build this crate, you need the following things installed:

- [`wasm-pack` CLI](https://rustwasm.github.io/wasm-pack/installer/)
- The `wasm32-unknown-unknown` compilation target (`rustup target add wasm32-unknown-unknown`)

### 🛠️ Build with `wasm-pack build`

```
wasm-pack build crates/flow-web
```

### 🔬 Test in Headless Browsers with `wasm-pack test`

```
wasm-pack test --headless --firefox crates/flow-web
```

## Capabilities

- `infer`: takes a JSON schema as input, and produces metadata about its inferred locations.
- `extend_read_bundle`: takes `read`, `write`, and `inferred` schemas (where `inferred` is null if no inferred schema is available), and returns an updated
  read-schema bundle which potentially inlines the write and inferred schemas.
- `get_resource_config_pointers`: takes a resource `spec` as string and produces metadata about the pointers
- `update_materialization_resource_spec`: takes `source_capture`, `resource_spec_pointers`, `collection_name`, and `resource_spec`. `resource_spec` is the object that is getting updated and the other fields are used to update that. It will return a copy of the `resource_spec` with the new fields populated.

## Making a Change?

Update the version in `crates/flow-web/Cargo.toml`.

The crate version needs to be updated in that file in order for the publication to the GitHub NPM registry to succeed. That registry doesn't allow overwriting
versions. When when originally set this up, it wasn't clear how to plumb through our dynamically generated versions through all the wasm/js/npm layers. It may
be possible to improve, but for now, flow-web will only be successfully published when the version number in Cargo.toml is incremented.
