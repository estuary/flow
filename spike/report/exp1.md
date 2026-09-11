# Experiment 1: launch through the podman API from the reactor's privilege level

**PASS.** From the fake reactor, the helper starts, the guest boots, and
connector-init answers on `<id>/sock/init.sock`. `CapEff` is podman's default
set plus `NET_ADMIN` and nothing else; `/dev/kvm` and `/dev/net/tun` are the
only added devices. The API service refused nothing on the helper's launch.

Measured at commit `6a2a47d6af6` (WP06's parent; the numbers precede the
commit), `spike/tasks/exp1-launch.sh`, guest
`ghcr.io/estuary/source-hello-world@sha256:96147403c20ca42faa2943b4d18d2bec8c4ff072892e7a09d240b43b674ad1db`.

## What ran

`flowctl preview` of `spike/catalog/capture-hello-world.flow.yaml` with
`FLOW_SANDBOX_SPIKE_POLICY` set, driven through `spike/tasks/fake-reactor.sh`:
the production reactor image, `--network=host`, podman's default capability set,
no `--cap-add`, no `--privileged`, no `--device`, and the host podman API socket
as its only privilege. 40 documents came back.

## The launch line the runtime emitted

Captured from `tracing::debug!` at `RUST_LOG=runtime_next=debug`, `<id>` folded:

```
run --rm --name=fs_<id> --network=flow-connectors --log-driver=none
  --device /dev/kvm
  --device /dev/net/tun
  --cap-add NET_ADMIN
  --sysctl net.ipv4.ip_forward=1
  --env=LOG_FORMAT=json --env=LOG_LEVEL=warn
  --memory 1280m --cpus 2
  --label=image=ghcr.io/estuary/source-hello-world:dev
  --label=task-name=acmeCo/hello-world
  --label=task-type=capture
  --mount=type=image,source=ghcr.io/estuary/source-hello-world:dev,destination=/rootfs,rw=true
  --mount=type=bind,source=<reactor>/fs_<id>/init,target=/init,ro
  --mount=type=bind,source=<reactor>/fs_<id>/venv,target=/venv,ro
  --mount=type=bind,source=<reactor>/fs_<id>/sock,target=/sock
  --mount=type=bind,source=<reactor>/fs_<id>/scratch,target=/scratch-backing
  localhost/flow-sandbox-helper:spike
  --policy /init/policy.json --memory-mib 1024 --vcpus 2 --disk-mib 4096
```

`1280m` is `FLOW_SANDBOX_SPIKE_MEMORY_MIB` (1024) plus
`FLOW_SANDBOX_SPIKE_MEMORY_OVERHEAD_MIB` (256), the placeholder constant until
WP09 measures it.

## What the running helper actually got

```
Privileged:  false
CapAdd:      [CAP_NET_ADMIN]
CapDrop:     []
Devices:     [{/dev/kvm /dev/kvm } {/dev/net/tun /dev/net/tun }]
Memory:      1342177280
NanoCpus:    2000000000

shim pid CapEff 00000000800415fb   (podman default 00000000800405fb, + bit 12 = CAP_NET_ADMIN)

/dev inside the helper:
core fd full kvm mqueue net null ptmx pts random shm stderr stdin stdout tty urandom zero

/proc/sys/net/ipv4/ip_forward = 1
```

`podman inspect` on 4.9.3 does not report sysctls at all, so `--sysctl` is
verified where it lands rather than where it was asked for, which is the better
question anyway.

## Refusals

One, and it is not the sandbox's:

```
docker command ["image", "inspect", "--platform=linux/amd64", "<image>"] failed:
  Error: unknown flag: --platform
```

`podman image inspect` has no `--platform` on 4.9.3. This is pre-existing
master code (`inspect_image`, commit `365987bfa6d`) which catches the failure
and retries without the flag, and it fires identically with the switch off. No
flag on the helper's own `podman run` was refused: a refusal there would have
stopped the launch, and the 40 documents rule that out.

## Notes

- The reactor's privilege level is enough, as PLAN expected. Nothing in the
  launch needed a root shell on the host. The reactor is root and the API
  service honors these flags for any client; what this pins is the exact flag
  set that works.
- `NET_ADMIN` is the one capability added, for the tap and its ruleset.
  `/dev/kvm` is the hypervisor and `/dev/net/tun` is the tap.
- The runtime created `<id>` before the launch and removed it after; the
  reactor directory count was unchanged across the run.
