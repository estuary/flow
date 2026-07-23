# Node README Format

Every concept node is a directory with a `README.md`. It leads with the glossary, then describes the concept in the present tense, then breadcrumbs out to code at the leaves.

## Concept node template

```md
# {Concept}

## Glossary

**{Term}**:
{One or two present-tense sentences: what it IS, not what it does.}
_Avoid_: {near-synonyms this term displaces}

**{Term}**:
{…}

## {Porcelain description}

{Present-tense prose describing the concept at this altitude — what it is, how
it relates to neighbouring concepts, its distinctive characteristics. Split into
`##` sections as the concept warrants. Link down to child nodes for deeper
specifics.}

## Where this lives

{Leaf breadcrumbs to the crate README(s) and source that implement the concept,
e.g. `crates/runtime-next/README.md`. Omit until the node is a leaf — a parent
node breadcrumbs to its children instead.}
```

## Rules

- **Glossary first.** The `## Glossary` heading is the first thing in the file — on-path for a reader who opened the node, and greppable across the tree.
- **Be opinionated about language.** One canonical term per concept; everything it displaces goes under `_Avoid_`. The banned words prime the distinction.
- **Present tense, tight definitions.** One or two sentences per term. Define what it IS. No history, no rationale, no "previously".
- **Only platform-specific terms.** General programming concepts don't belong, however heavily the platform uses them. Ask: is this a concept unique to the Estuary platform, or a general one? Only the former.
- **Altitude discipline.** A node describes its concept at one altitude; deeper specifics descend into child nodes. Don't flatten the whole subtree into one README.

## Root README format

`docs/platform/README.md` is the living concept map. It carries:

- A short present-tense overview of the platform.
- `## Glossary` — the platform-wide, cross-cutting terms (the ones no single trunk owns), with `_Avoid_` lists.
- `## Concept map` — the trunks, one line each on what the trunk owns, linking down.
