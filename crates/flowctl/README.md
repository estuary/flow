# Flowctl

**The command line interface for Estuary Flow**

### Installing `flowctl`

**Download the binary for your OS**

- All Mac systems with MacOS 11 (Big Sur) or later, including both Intel and M1
  Macs:
  [Download here](../../../../releases/latest/download/flowctl-multiarch-macos)
- Linux (x86-64 only), all distributions:
  [Download here](../../../../releases/latest/download/flowctl-x86_64-linux)

Make the file executable, rename it, and put it somewhere on your `PATH`, for
example:

```console
chmod +x ~/Downloads/flowctl-multiarch-macos
mv ~/Downloads/flowctl-multiarch-macos /usr/local/bin/flowctl
```

Verify that it's working by running `flowctl --version`.

### Use the `flowctl` CLI:

**Authentication to Estuary Flow**

1. Visit (https://dashboard.estuary.dev/admin) and login.
2. Find the "Access Token" at the bottom of the page, and copy it.
3. Run `flowctl auth token --token <paste-your-token-here>`

You're ready to go!

**Authenticate to a local instance of Flow**

Authenticate as "bob@example.com" with your local control-plane API:

```console
flowctl auth develop
```

Or, grab an access token from the Admin page and pass it in:

```console
flowctl auth develop --token your-access-token
```

### Usage

Create a draft and publish specifications

```console
flowctl draft create
flowctl draft author --source ~/estuary/flow/examples/citi-bike/flow.yaml
flowctl draft publish
```

## Building from source

First, make sure have rust installed.

```
make flow-cli
```

Now the binary is available at `target/release/flowctl`.

Note that if you are using a recent version of git with `index.skipHash` enabled you may need to set this value to false in order to build. To fix this, without any changes in your local staging, run:

```shell
git config index.skipHash false
rm .git/index
git reset --hard
```
