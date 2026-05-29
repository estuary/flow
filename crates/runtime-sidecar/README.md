# runtime-sidecar

Production sidecar process for the runtime-v2 architecture
(`plans/runtime-v2/plan.md`). One per reactor machine, supervised by
systemd, hosting two gRPC services on a fixed fleet-wide port:

- **Shuffle Leader** — `runtime_next::leader::Service`, the per-task
  Join rendezvous and HeadFSM/TailFSM coordination for tasks whose
  shard zero is on this machine.
- **Shuffle** — `shuffle::Service`, the Session/Slice/Log RPCs.

## Listeners

`--listen-port` binds a TCP listener at `[::]:<port>`. TLS is on if
`--certificate-file` and `--certificate-key-file` are both provided.

## Auth

`--data-plane-auth-keys` is whitespace- or comma-separated base64
HMAC keys, matching gazette's `auth-keys` semantics. The first key
signs outbound requests; all keys verify inbound traffic (rotation).

Inbound RPCs are authenticated and authorized: every request must
carry a JWT issued by `--data-plane-fqdn` and bearing the service's
capability — `LEAD` for the Leader, `SHUFFLE` for Shuffle — or it is
rejected before its handler runs. Each handler additionally enforces a
scope check: the token's selector must authorize the shard the handler
operates on (shard zero of a Leader/Session join, or the hosted shard of
a Slice/Log). Because a task's shards share an `id` prefix and the bearer
is scoped to that prefix, this gates access at task granularity.

The loopback admin surface is a separate server and
is intentionally not authenticated (bound only to loopback).
