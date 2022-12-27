# Flow all up in your browser

This is the source for the `flow-web` NPM package, which exposes Javascript/Typescript bindings to Flow library functions in Web Assembly (WASM).
The gist is that we compile this Rust crate to WASM and then generate the corresponding JS/TS files using `wasm-bindgen`. We use `wasm-pack` to put
everything together into an NPM package that works with Webpack, and publish that to Github packages.

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

Currently, this only exposes a basic schema inference function, to prove out the functionality and give us a starting point.
We'll very likely need to add functionality in order to make this truly useful by the UI.

