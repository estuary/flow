---
name: platform-docs
description: Maintain docs/platform/ — the porcelain, present-tense description of the Estuary platform's architecture, operational components, and ubiquitous language. Use when a change introduces, renames, or alters a platform concept, its semantics, or the shared vocabulary, or when another skill needs to record platform architecture or language. Do not use for code-layout docs (crate READMEs) or decision history (git/issues).
---

# Platform Docs

Actively maintain `docs/platform/` as you design and build — the discipline of
keeping the platform's porcelain description true to the platform as it exists
today. Read [DOCTRINE.md](./DOCTRINE.md) for what these docs are and how every
node is shaped; this skill applies that doctrine to write. (Merely *reading*
these docs for vocabulary is not this skill — that's a one-line habit any skill
can do. This skill is for when you're changing the model, not consuming it.)

## Maintaining the docs

- **Write the moment it crystallises.** When a concept resolves, capture it right
  there — resolve its language in the glossary first, then describe it. Don't
  batch changes up.
- **Create files lazily.** A node only when you have something porcelain to
  write.
- **Correct drift to the present.** If a doc has drifted from the code, fix it to
  what *is* now; don't narrate the drift.
