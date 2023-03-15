# Running Flow locally


## Dependencies

### Clone these repositories locally:

- [github.com/estuary/flow](https://github.com/estuary/flow).
- [github.com/estuary/data-plane-gateway](https://github.com/estuary/data-plane-gateway).
- [github.com/estuary/ui](https://github.com/estuary/ui).
- [github.com/estuary/config-encryption](https://github.com/estuary/config-encryption).

### Required tools and libs:

Where links are provided, just follow the linked instructions to install the latest version. If there's no link, then use whatever is provided by your package manager of choice and it'll probably be fine.

- [Supabase CLI](https://github.com/supabase/cli)
- [SOPS CLI](https://github.com/mozilla/sops)
- [Deno](https://deno.land/manual/getting_started/installation)
- Rust and Cargo (see below)
- [Golang](https://go.dev/doc/install)
- tmux
- clang
- curl
- g++
- gcc
- git
- libreadline-dev
- libsqlite3-dev
- libssl-dev
- make
- musl-tools
- openssl (we need both the libs and the CLI)
- pkg-config
- protobuf-compiler

### Install rust:

The recommended way to install rust is using `rustup`. Don't use whatever is provided by `apt`.
We want rustup because it makes it easy to install other compilation targets.

```console
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"
rustup target add x86_64-unknown-linux-musl
rustup target add wasm32-unknown-unknown
```

## Build and preparation

### Start Supabase:

From the root of the `flow` repo, run:

```console
supabase start
```

If supabase is already running, you can run the following command instead to just reset the state of the database.

```console
supabase db reset
```

- You can access the Supabase UI at (http://localhost:5433)
- Access the email testing server at (http://localhost:5434/monitor), which is used for local logins using "magic links"

### Build Flow binaries

From the root of the `flow` repository, run `make` and ensure that it completes successfully. If there's a problem here, feel free to ask for help, as it's possible these instructions have become out of date. This will take a while the very first time you run it, but should be a bit faster on subsequent runs.

This creates a directory of binaries `${your_checkout}/.build/package/bin/` which the control-plane agent refers to as `--bin-dir` or `$BIN_DIR`.

## Start Flow

From the root of the `flow` repository, run `./local/start-flow.sh`. This will open a tmux session where each component is running in its own window (tab). Here's a [tmux quick reference](https://quickref.me/tmux#tmux-shortcuts). If you're unfamiliar with tmux key combinations, most of them involve hitting `ctrl+b` and then (after releasing those) hitting another key, like `n` to move to the next window.

You can use `ctrl+b` `n` to cycle between windows and ensure that all the components startup correctly. This make take a while the first time you run it.

The Flow UI should open in your browser automatically.

### Logging in locally

To login locally, you need to use the "magic link" method. Oauth does not work locally. You can enter any email address you like, and all emails will be sent to [http://localhost:5434/monitor]. After it says "email sent", go to that address and click the link in the email to complete the login process.

### Stopping Flow

- Kill the tmux session (`crtl+b` `:` then type `kill-session` and hit enter)
- run `supabase stop` from the root of the `flow` repo
- Local Flow instances sometimes leave dangling docker containers, which you can cleanup using `docker ps` and `docker stop`.

