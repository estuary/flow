# Platform Docs Doctrine

What `docs/platform/` is and how every node is shaped. Invocation-neutral: the
[platform-docs](./SKILL.md) skill applies this doctrine to write inline; other
skills point here to shape doc changes in the same idiom without writing.

## Porcelain

**Porcelain** describes the present surface — not the plumbing beneath it (the
code) nor the history behind it (how it came to be this way). History lives in
git commits, issues, and specs. These docs describe only what *is*, in the
present tense. When you feel the urge to explain how it got this way, or what it
used to be — stop. That belongs in git.

## The hierarchy

`docs/platform/` is organized by **concept and function**, never by code layout.
A concept maps to code many-to-one and only at the leaves, so a crate refactor
never reshapes the tree.

- Each concept is a **directory + `README.md`**, its README a porcelain
  description at that altitude. Children hold deeper specifics.
- Descend from general to specific; a reader stops at the altitude that answers
  their question.
- Code appears only at **leaf breadcrumbs** — a link out to the crate README or
  source that implements the concept.

The live concept map is `docs/platform/README.md`. Node READMEs follow
[NODE-FORMAT.md](./NODE-FORMAT.md).

## The glossary is the consistency oracle

Every node README opens with `## Glossary`. Before naming anything, check whether
the platform already has a word for it. Pick one canonical term and list the
near-synonyms it displaces under `_Avoid_` (`**tenant** … _Avoid_: account,
org`) — the banned words prime a reader who is minting names to feel the
distinction. When a term conflicts with the glossary, call it out: "the glossary
says `tenant`; you wrote `account` — which is it?"

## Stay conceptual

Describe what a concept *is*, how it relates to others, and its vocabulary. Leave
code structure — a crate's types, entry points, file layout — to crate READMEs,
and point to them at the leaves. Test: if a fact survives a crate refactor, it
belongs here; if it's about where the code lives, it belongs in a README.

## Cross-reference against code

The porcelain must match reality. When you state how something works, verify the
code agrees. If code and doc contradict, the code wins — correct the doc to the
present, and surface the gap.

## Breadcrumbs

Wire the tree so an exploring agent can walk it: parents link down to children,
leaves link out to the implementing crate READMEs and source. A concept the
reader can't navigate to is a concept they won't find.
