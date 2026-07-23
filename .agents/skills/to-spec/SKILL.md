---
name: to-spec
description: Synthesize the current conversation into a spec, and publish it as an estuary/flow GitHub issue.
disable-model-invocation: true
---

Take the current conversation and codebase understanding
and produce a spec (you may know this as a PRD), then publish it as a GitHub issue.
Do NOT interview the user to gather the spec — synthesize what you already know.
The only exceptions are the two confirmation checkpoints in steps 2 and 3.

## Process

### 1. Gather context

If you haven't already, explore the repo to understand the current state,
including relevant `docs/platform/` nodes read through the
[platform docs doctrine](../platform-docs/DOCTRINE.md) lens.
The glossary is your consistency oracle.
Use the platform's canonical vocabulary throughout the spec.

### 2. Sketch seams

Sketch out the seams at which you're going to test the feature.
Existing seams should be preferred to new ones.
Use the highest seam possible.
If new seams are needed, propose them at the highest point you can.
The fewer seams across the codebase, the better - the ideal number is one.
Check with the user that these seams match their expectations.

### 3. Consolidate the docs delta

Consolidate the **docs delta**: the *manifest* of what changes and, in more detail,
the *glossary and ubiquitous-language* changes that force it.
If `/grill-with-docs` ran, its accrued delta is your primary input.
Otherwise derive it best-effort from the conversation context,
considered through the platform docs doctrine.
Check with the user that the consolidated docs delta
matches their expectations before continuing.

### 4. Write the spec

Write the spec using the template below
and publish it as a GitHub issue in `estuary/flow`.
Always apply the `spec` label. Add area labels as they apply:
`data-plane`, `control-plane`, `flowctl`, `chore`, and `integrations`
(for changes with connector impact).

<spec-template>

## Problem Statement

The problem the user is facing, from the user's perspective.

## Solution

The solution to the problem, from the user's perspective.

## User Stories

A LONG, numbered list of user stories, each in the format:

1. As an <actor>, I want a <feature>, so that <benefit>

<user-story-example>
1. As a mobile bank customer, I want to see balance on my accounts, so that I can make better informed decisions about my spending
</user-story-example>

This list of user stories should be extremely extensive and cover all aspects of the feature.

## Implementation Decisions

A list of implementation decisions that were made. This can include:

- The modules that will be built/modified
- The interfaces of those modules that will be modified
- Technical clarifications from the developer
- Architectural decisions
- Schema changes
- API contracts
- Specific interactions

Do NOT include specific file paths or code snippets. They may end up being outdated very quickly.

Exception: if a prototype produced a snippet that encodes a decision more precisely than prose can (state machine, reducer, schema, type shape), inline it within the relevant decision and note briefly that it came from a prototype. Trim to the decision-rich parts — not a working demo, just the important bits.

## Testing Decisions

A list of testing decisions that were made. Include:

- A description of what makes a good test (only test external behavior, not implementation details)
- Which modules will be tested
- Prior art for the tests (i.e. similar types of tests in the codebase)

## Platform Docs Delta

The manifest of required changes to `docs/platform/`,
for the implementor to write via the `platform-docs` skill.
Not the README prose itself.

- **Manifest**: each affected node, the nature of the change, and the decision that forces it.
- **Glossary & ubiquitous language**: in detail — new canonical terms with their
  definitions, renames, semantic shifts, and the near-synonyms each displaces
  (`_Avoid_`). Note any term that conflicts with the present glossary.

Omit this section only when the change touches no platform concept or vocabulary.

## Out of Scope

A description of the things that are out of scope for this spec.

## Further Notes

Any further notes about the feature.

</spec-template>
