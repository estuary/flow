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
signs outgoing `/authorize/task` requests issued by the leader and
shuffle to obtain Gazette journal tokens. (Incoming-gRPC verification
against the full key list is wired in a follow-up change.)
