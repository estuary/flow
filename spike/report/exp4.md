# Experiment 4: derive-python end to end, permissive network

**PASS**, with a caveat on what "permissive" meant. The derivation produced its
four documents, and they are byte-identical to the unsandboxed run. `uv`
resolved a name, fetched pandas 3.0.5 from PyPI over TLS from inside the guest,
built a 180 MiB environment on the scratch disk, and the module imported and ran
under connector-init over vsock.

**Caveat:** this ran under the **placeholder** egress scripts, not WP02's
ruleset, which does not exist yet. See "What 'allow-all' meant here" below.
Experiment 4 should be rerun under the real ruleset once WP02 lands.

Measured at commit `6a2a47d6af6` (WP06's parent; the runs precede the commit),
`spike/tasks/exp4-derive.sh`, connector
`ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7`.

## What ran

`spike/catalog/derive-pandas.flow.yaml`: a Python derivation over
`acmeCo/events` whose only declared dependency is pandas, previewed with
`spike/catalog/policy-allow-all.json` (`egress: public`, `allowAll: true`) and a
four-document fixture.

```
["acmeCo/pandas-rollup",{"_meta":{...},"characters":19,"message":"Hello number 0 here","words":4}]
["acmeCo/pandas-rollup",{"_meta":{...},"characters":19,"message":"Hello number 1 here","words":4}]
["acmeCo/pandas-rollup",{"_meta":{...},"characters":19,"message":"Hello number 2 here","words":4}]
["acmeCo/pandas-rollup",{"_meta":{...},"characters":19,"message":"Hello number 3 here","words":4}]
```

Identical, with no masking at all, to the same preview with the switch off.

Any one of these failing produces no documents: Python running under libkrun,
the tap, the masquerade, DNS, TLS to PyPI, uv writing to the scratch disk, and
connector-init carrying the derive protocol over vsock.

## `/scratch` footprint

Reported by the derivation itself at import - the disk is destroyed with the VM,
so nothing can measure it afterwards.

```
used    = 183,164 KiB      (179 MiB)
total   = 4,112,096 KiB    (the --disk-mib 4096 scratch disk)

.tmp<XXXXXX>   = 134,510 KiB    the built virtual environment
archive-v0     = 122,054 KiB    uv's unpacked wheel cache
simple-v21     =  13,919 KiB    uv's index metadata cache
wheels-v6      =      18 KiB
sdists-v9, interpreter-v4, CACHEDIR.TAG, .lock, .gitignore, lost+found = 0 KiB
```

**This does not size `diskMib`.** Production guests receive a prebuilt
dependency set as a read-only block image on `/dev/vdb` (experiment 5b) and will
not run `uv` at all. The number is here because it says what a guest that *does*
build its own environment costs: roughly 180 MiB for one mid-sized dependency,
of which the venv and uv's cache are each about half. `TMPDIR` and
`UV_CACHE_DIR` both point at `/scratch` (flow-init), which is why none of this
landed in podman's container layer on host disk.

## What "allow-all" meant here, and what it did not

WP02 has not run, so the helper is still executing the placeholders in
`spike/helper/stubs/`. WP06 changed both of them, minimally, because without the
changes the guest has no working network at all and this experiment could not
run:

- **`flow-sandbox-egress`** gained a `nat postrouting oifname "eth0" masquerade`
  chain. Without it the guest's packets leave the tap with source `192.0.2.2`,
  go out the uplink untranslated, and no reply can return. This is not policy -
  CONTRACTS already assigns masquerading to the real ruleset ("The helper
  masquerades guest traffic out of eth0") - it is the plumbing every policy
  needs. The forward chain still accepts everything and enforces nothing.
- **`flow-sandbox-resolver`** previously exited 0 immediately. flow-init writes
  `nameserver 192.0.2.1` into the guest, so with nothing listening there the
  guest cannot resolve a name even on an open network. It now forwards UDP/53 to
  the helper's own upstream with socat.

So what this experiment exercised is an **unenforced** network that happens to
work, which is what `allowAll: true` reduces the real pair to. What it did
**not** exercise, and what WP07 must:

- the baseline denylist, `declaredCidrs`, and the rate limits,
- the resolver checking answers against the denylist and refusing the whole
  answer on a hit, feeding the `resolved` nft set before replying, and clamping
  TTLs,
- AAAA answers being emptied. The placeholder forwards them verbatim, so the
  guest saw four AAAA records for pypi.org alongside the A records. IPv6 is off
  in the guest and Python picked IPv4, so nothing failed - but a client that
  strictly preferred AAAA would have, and the real resolver is what prevents
  that.

Both placeholders remain WP02's to replace wholesale; nothing was added to them
that WP02 needs to preserve except the masquerade, which it owes anyway.

## Notes

- **pyright strict mode shapes what a derivation module may contain.**
  derive-python type-checks the module with pyright at
  `typeCheckingMode: strict`, so untyped pandas fails the build before anything
  runs. The spec therefore declares `pandas-stubs` alongside `pandas`, and the
  module uses `Series.sum()` rather than `.iloc[0]`, whose type pandas-stubs
  leaves partially unknown. Worth knowing before a customer hits it: this is a
  real constraint on customer Python, and it is unrelated to the sandbox.
- **A derivation module must not print to stdout.** stdout is the derive
  protocol's own channel and connector-init parses every line of it as a JSON
  response; the first version of the footprint probe printed there and killed
  the session with `could not parse "scratch-footprint: ..." into JSON
  response`. Also unrelated to the sandbox, and also a thing a customer will do.
- Spec and Validate ran unsandboxed here too, through the legacy `runtime`
  crate, exactly as in experiment 3. For a Python derivation that means the
  customer's module was type-checked and validated - and its dependencies
  fetched - outside the sandbox. See `exp3.md` "Scope" for the phase-2 open
  problem; it matters more here than it does for a Go connector.
