## What is a Plan?

Plans are living documents that provide a roadmap for current and past projects. They contain an exectuive summary; background; requirements; links to relevant types and functions with brief details of relevance; risks; architecture and diagrams; implementation & testing plans; and a record of key decisions made.

Plans are targeted to expert developers and omit excessive background.

Format: GitHub Markdown with structured lists, bullets, links, progress checklists, and Mermaid diagrams. See [TEMPLATE.md](TEMPLATE.md). Plans live in the [plans](plans) directory.

## Why Plans?

* Provide shared structure and context for humans and AI agents
* Serve as historical artifacts for future reference

## Plan Lifecycle

- Plans are written by developers and AI agents.
- Plans are sometimes reviewed in PRs by stakeholders
- Plans are updated continuously as design and implementation progress
  - They're kept "evergreen" and reflect the current project plan
  - Significant changes to design or implementation generate historical decision records
    - Examples: new requirements, a better design, or a new blocker.
    - AI assistants create decision records only when directed by developers
- Checklists track implementation and testing progress
