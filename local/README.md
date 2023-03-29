# Running Flow locally


## Dependencies

### Clone these repositories locally:

When running flow locally, we require and assume that these repos are all cloned under the same parent directory, and have names consistent with the repo names in github (the last part of the URL).

- [github.com/estuary/flow](https://github.com/estuary/flow).
- [github.com/estuary/data-plane-gateway](https://github.com/estuary/data-plane-gateway).
- [github.com/estuary/ui](https://github.com/estuary/ui).
- [github.com/estuary/config-encryption](https://github.com/estuary/config-encryption).

### Required tools and libs:

Where links are provided, just follow the linked instructions to install the latest version. If there's no link, then use whatever is provided by your package manager of choice and it'll probably be fine.

- [Tilt](https://tilt.dev/) (see below)
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

## Using Tilt

The fancy new way to start flow locally is to run `tilt up` from the root of the Flow repo. You just need to have all of the dependencies described above, and `tilt up` should take care of the rest.

**Installing Tilt** can be done by just running the `curl ... | bash` instructions [on their website](https://docs.tilt.dev/install.html). Note that you shouldn't need to follow any of the Kubernetes-related instructions, since we're not using k8s with Tilt. Just installing the `tilt` binary ought to be enough.

This is still pretty new, so the old `start-flow.sh` is still in place for the time being.

To stop Flow, just CTRL+c the terminal where you started it.

### State

When using Tilt, data is persisted across restarts. Control-plane data is still just stored in the containers that are started by `supabase start`.
By default, the data-plane data is stored in `~/flow-local/`. You can change the directory that's used for data-plane data by setting the `FLOW_DIR` env var.
It's on you to ensure that the control-plane and data-plane states are consistent. If you stop supabase, then you'll probably want to also run `rm -rf ~/flow-local`, and vice versa.

## Using `start-flow.sh`

**1: Start Supabase**

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

**2: Build Flow binaries**

From the root of the `flow` repository, run `make` and ensure that it completes successfully. If there's a problem here, feel free to ask for help, as it's possible these instructions have become out of date. This will take a while the very first time you run it, but should be a bit faster on subsequent runs.

This creates a directory of binaries `${your_checkout}/.build/package/bin/` which the control-plane agent refers to as `--bin-dir` or `$BIN_DIR`.

**3: Start Flow**

From the root of the `flow` repository, run `./local/start-flow.sh`. This will open a tmux session where each component is running in its own window (tab). Here's a [tmux quick reference](https://quickref.me/tmux#tmux-shortcuts). If you're unfamiliar with tmux key combinations, most of them involve hitting `ctrl+b` and then (after releasing those) hitting another key, like `n` to move to the next window.

You can use `ctrl+b` `n` to cycle between windows and ensure that all the components startup correctly. This make take a while the first time you run it.

The Flow UI should open in your browser automatically.

**Stopping Flow**

- Kill the tmux session (`crtl+b` `:` then type `kill-session` and hit enter)
- run `supabase stop` from the root of the `flow` repo
- Local Flow instances sometimes leave dangling docker containers, which you can cleanup using `docker ps` and `docker stop`.

## Notes on 
### Logging in locally

To login locally, you need to use the "magic link" method. Oauth does not work locally. You can enter any email address you like, and all emails will be sent to [http://localhost:5434/monitor]. After it says "email sent", go to that address and click the link in the email to complete the login process.

### Data-plane-gateway TLS certificates

Data-plane-gateway requires TLS, even locally. Starting Flow with either method will generate a self-signed certificate automatically and store it in the root of the data-plane-gateway local repository.
In order for the UI to work correctly, you'll need to tell your browser to trust that certificate. To do that, navigate to (https://localhost:28318/) and click through the warnings to trust the certificate.
As long as you don't delete and regenerate the certificates, you should only need to do this once per year (the certificate is valid for a year).

Once you trust the certificate, you should start seeing shard statuses in the UI.


