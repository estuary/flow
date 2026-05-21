# service-kit

Building blocks for the operational surface of a long-running async service:
a loopback HTTP port exposing what the process is doing right now, with
controls to debug it without restarting.

Service-agnostic. Used by Estuary reactors and runtime-next; nothing here
knows about Flow.

## Surface

A service constructs a [`Registry`], wires its tracing/event/metrics layers
in, and serves the admin router on a loopback port. Every spawned unit of
work registers a [`HandlerGuard`]; its lifecycle, identity, and recent events
become visible on the dashboard.

| Module             | Role                                                                                    |
| ------------------ | --------------------------------------------------------------------------------------- |
| [`handlers`]       | `Registry` / `HandlerGuard` — in-flight handler inventory plus a recently-finished ring |
| [`admin`]          | HTML dashboard, JSON views, per-handler drill-down, trace-override `POST` endpoint      |
| [`trace`]          | Per-handler `tracing` verbosity override — additive filter composed with the base       |
| [`event`]          | Opt-in per-handler event tracks (named ring buffers) + `event!` macro; lazy capture     |
| [`metrics`]        | Prometheus `/metrics` exporter folded into the admin router; histogram upkeep tick      |

## Entry points

- `Registry::new()` / `Registry::register(kind)` — register a handler; run
  its body inside `guard.span()` so tracing/event layers can find it.
- `admin::serve(name, registry, addr, shutdown)` — bind the loopback admin
  port and serve until shutdown.
- `trace::layer_filter(base, registry)` — wrap a `tracing_subscriber`
  `EnvFilter` so operator overrides bypass it.
- `event::layer(registry)` — install alongside `fmt` so `event!` calls
  capture into per-handler tracks.
- `metrics::install_recorder()` — idempotently install the global
  `metrics` recorder; called transitively by `admin::build_router`.

## Non-obvious details

- **No auth.** Bind admin on loopback only.
- **Trace-override is additive** — it raises verbosity for one handler but
  never suppresses what the base filter would keep. Cost when no override
  is set is one extra `enabled()` check per disabled callsite (atomic load,
  short scope walk only inside a handler span).
- **Handler spans must always be created.** `OverrideFilter` short-circuits
  to `true` for the `service_kit::handler` target — that's where override
  state is hung. Don't filter the target out at the base.
- **Register inside the spawned task.** A handler span captures the current
  `tracing` dispatcher at creation and `tokio::spawn` doesn't propagate it, so
  registering before a spawn can close the span against the wrong registry and
  panic. See `Registry::register`.
- **`event!` capture is lazy.** A literal message stays `&'static str`; an
  interpolated message stores `(template, captured args)` and is rendered
  only when read. Values must be cheap-to-capture types (numbers, bools,
  `&'static str`, `String`, `Arc<str>`, `Clock`) or a `lazy`/`json`/`debug`
  thunk — borrowed `&str` or `?x`/`%x` formatters won't compile; the call
  site converts. See [`event`] for the full contract.
- **`event!` is a no-op outside a handler span** *and* under any subscriber
  that isn't a `tracing_subscriber::registry::Registry`. The accompanying
  `tracing` event still fires.
- **Prometheus histogram upkeep** isn't driven by scrapes; `install_recorder`
  spawns its own tick because we own the HTTP surface (the upstream
  `PrometheusBuilder::install` convenience constructor isn't used). Idle-
  metric pruning *is* scrape-driven, so it only fires when a scraper polls.
- **`metrics` recorder install is process-global and panics on conflict.**
  Service-kit owns that slot.

[`Registry`]: src/handlers.rs
[`HandlerGuard`]: src/handlers.rs
[`handlers`]: src/handlers.rs
[`admin`]: src/admin.rs
[`trace`]: src/trace.rs
[`event`]: src/event.rs
[`metrics`]: src/metrics.rs
