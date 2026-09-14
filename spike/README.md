# spike/

Throwaway working directory for the libkrun sandbox spike. Nothing under
`spike/` merges to master. See `PLAN.md` for what we are proving and why.

**Status: complete (2026-09-14). Decision: go.** The report is
`report/REPORT.md`; the ledger that got there is `STATUS.md`.

## Layout

- `PLAN.md`        the agreed plan: environment, thing under test, experiments,
                   report, decision. Read the sections your brief names.
- `CONTRACTS.md`   interfaces between work packages. Binding. Deviations go in
                   STATUS.md, not into other packages' code.
- `wp/WPnn.md`     one brief per work package.
- `STATUS.md`      the ledger. Append when you finish; the master thread reads
                   only this.
- `tasks/`         push-button scripts each package leaves behind. One command
                   per thing; no remembering how the system works.
- `report/`        per-experiment results (`expN.md`) and raw data (`data/`).
- everything else  package source trees (`helper/`, `flow-init/`, `egress/`,
                   `stub-helper/`, `derived/`, ...).

## Session protocol

Each work package runs in its own Claude Code session with a small context,
one at a time, in this checkout, on `daveg/libkrun-spike`.

Open the session with this prompt, filling in NN:

> Read spike/README.md, spike/CONTRACTS.md, and spike/wp/WPNN.md, then only
> the PLAN.md sections and source files WPNN.md names. Work only within the
> paths WPNN.md allows. When done, run its verification, append a dated entry
> to spike/STATUS.md (what shipped, verification output, any contract
> deviation, questions for the master thread), commit with prefix
> `spike(wpNN):`, and stop.

The master planning thread reads STATUS.md between sessions and decides what
runs next.

Rules that hold in every session:

- Never write customer data (tenant prefixes, task names, hostnames, values)
  into any file. Use `acmeCo/` and invented names.
- Never write a line beginning with a space to stderr from anything that runs
  inside the helper or guest. The reactor treats a leading space byte as
  connector-init's readiness signal.
- Numbers go in `report/`, not in chat.
