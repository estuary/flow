# Experiment 4: derive-python end to end, permissive network

**PASS, under WP02's real ruleset.** The derivation produced its four documents,
and they are byte-identical to the unsandboxed run. `uv` resolved a name, fetched
pandas 3.0.5 from PyPI over TLS from inside the guest, built a 180 MiB
environment on the scratch disk, and the module imported and ran under
connector-init over vsock.

WP06 first ran this under the **placeholder** egress scripts, because WP02 had
not landed, and recorded it as provisional. WP07 reran it unchanged against the
real `flow-sandbox-egress` and `flow-sandbox-resolver`, with the same
`policy-allow-all.json`, and it passes: same four documents, same parity with
the unsandboxed arm, same pandas 3.0.5 from PyPI. The provisional is closed. See
"What 'allow-all' means here" below for what that policy does and does not
switch off - the answer is less than the name suggests.

Measured at commit `6a2a47d6af6` (WP06's parent) and rerun at `8703b322029`
(WP07's parent, the first commit at which the real binaries are in the helper
image), `spike/tasks/exp4-derive.sh`, connector
`ghcr.io/estuary/derive-python@sha256:c26548740a9e967274f6d7b9c79bed73630bf61bd187c3afca7564577ef364c7`
in both runs.

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

## What "allow-all" means here

`allowAll: true` adds one accept at the head of `egress_accept` and changes
nothing else, so the run is not on an unenforced network. The forward chain the
rerun loaded, verbatim:

```
chain forward {
        type filter hook forward priority filter; policy drop;
        iifname "tap0" ip saddr != 192.0.2.2 counter drop comment "anti-spoof"
        ct state established,related counter accept comment "replies"
        meta nfproto ipv6 counter drop comment "no-ipv6"
        meta l4proto != { tcp, udp } counter drop comment "tcp-udp-only"
        ip daddr @baseline counter drop comment "baseline"
        tcp dport 25 counter drop comment "smtp"
        ct state new counter jump egress_accept comment "egress"
        counter comment "forward-drop"
}
chain egress_accept {
        counter accept comment "allow-all"
        ip daddr @resolved counter accept comment "resolved"
}
```

So the rerun did exercise the anti-spoof rule, the baseline denylist, the
IPv6 and non-TCP/UDP drops, the tcp/25 drop, the masquerade, the input chain
that lets only the guest's DNS reach the helper, and the output chain that lets
nothing open a connection to the guest. What `allowAll` removes is the
requirement that a destination be *named*: with it set, `@resolved` and
`@declared` no longer gate anything, so an address that was never resolved is
still reachable as long as it is outside the baseline.

The real resolver is also what answered, which is the other half of the earlier
caveat. It gates every A answer against the baseline, empties AAAA answers,
clamps TTLs and feeds `@resolved` before replying - and pypi.org still resolved
and still fetched. WP06 had recorded that the placeholder forwarded pypi.org's
four AAAA records verbatim and that a client strictly preferring AAAA would have
failed; under the real resolver those records are gone.

What this experiment still does not exercise, and experiment 6 does: DNS-gated
reachability with `allowAll` off, `declaredCidrs`, and the rate limits.

## Notes

- **pyright strict mode shapes what a derivation module may contain.**
  derive-python type-checks the module with pyright at
  `typeCheckingMode: strict`, so untyped pandas fails the build before anything
  runs. The spec therefore declares `pandas-stubs` alongside `pandas`, and the
  module uses `Series.sum()` rather than `.iloc[0]`, whose type pandas-stubs
  leaves partially unknown. Unrelated to the sandbox and pre-existing, but see
  the open problem below: it is the friction this experiment actually hit.
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

## Open problem, and not one the spike should decide: pyright `strict`

**Owner: derive-python. Flagged here, not proposed.**

derive-python hardcodes `"typeCheckingMode": "strict"`
(`crates/derive-python/src/lib.rs:381`), force-installs pyright as a required
dependency (`:400`), and fails the **Validate** RPC on any finding (`:262`),
which fails the publish or preview. It cannot be relaxed from a catalog spec:
`DeriveUsingPython` carries only `module` and `dependencies`, and the connector
writes its own `pyrightconfig.json` into the generated project.

In strict mode `reportUnknownMemberType` and `reportUnknownVariableType` make a
dependency without type information poison every expression that touches it. The
effective constraint on customer Python is therefore not "write typed code" but
"only use libraries that ship `py.typed` or have a stubs package" - which sits
across the premise of this whole spike, that customers bring arbitrary Python
with arbitrary dependencies.

**What the evidence here actually covers: one library.** pandas, and it was
resolvable by declaring `pandas-stubs`. This spike does not survey how much of
the ecosystem is affected, and that survey is what any decision should rest on.

It is recorded because the sandbox is the thing that makes customer Python a
product surface, so this is the moment the constraint stops being theoretical.
Changing it is a decision for whoever owns derive-python, on someone's explicit
ask. Nothing in this spike touches that connector.
