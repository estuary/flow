# Flowctl

**The command line interface for Estuary**

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

**Authentication to Estuary**

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

### Profiles

A profile is a separate `flowctl` configuration, such as a second account or a
development endpoint. Each profile is a `<name>.json` file in the flowctl
config directory: `~/Library/Application Support/flowctl/` on macOS and
`~/.config/flowctl/` on Linux. `flowctl --profile <name> auth login` creates
one.

```console
flowctl profile list      # profiles and their accounts; * marks the active one
flowctl profile current   # the active profile, and what chose it
flowctl profile use work  # save `work` as the active profile
flowctl profile delete old  # delete a profile (asks first; --yes skips)
```

The active profile is the first of: the `--profile` flag, `$FLOWCTL_PROFILE`,
the profile saved by `flowctl profile use`, and `default`. The saved name is in
the file `active-profile` in the config directory. `profile delete` refuses
the active profile and the saved profile.
