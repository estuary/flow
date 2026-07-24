---
name: grill-transcript
description: Build a summarized transcript of a grilling session, written to a file or GitHub issue.
disable-model-invocation: true
---

The current conversation includes a `/grilling` session
(if it doesn't, tell the user this skill must follow a grilling, and stop).

Take the current conversation and codebase understanding
and produce a **transcript** of the grilling, recorded into a file or GitHub issue.
These transcripts are a record to inform the user's peers of the conversation:
the decision tree branches that were considered, relevant key facts of the codebase,
and opinions offered by the user in answers
(**especially** when they diverge from garden-path "agree" responses).

Transcripts are a factual, historical record of a conversation with an agent that built a shared
understanding. That shared understanding is useful, but other parties will have their own
opinions and a transcript *must* not over-represent the "settled" nature of decisions.
Peer parties will form their own opinion.

## Writing the Transcript

Transcripts follow the linear history of the conversation and are
structured as a written Q&A, interspersed with select findings from code
and other user-provided artifacts that motivate and ground the questions.

<spec-template>

# Grilling Transcript
Alice ran a grilling session discussing a deadlock in the video service.

### Q1: Fix via patches, or a structural change?
(Summary of decision branch)

#### F1: `VideoStream` and `VideoComment` embed independent mutexs
(Brief discussion of relevancy)

#### F2: Finding
(Brief discussion of relevancy)

#### A2: Structural change
(If the user provided an interesting rationale or conversation guidance, quote them. Don't quote on a garden-path "agreed").

### Q2: Coupled lifetime?
(Summary)

#### F3: Finding

#### A2: Answer

## Outcome

(Terse summary of shared understanding that was reached).

</spec-template>

## Publish

At the user's direction, publish the transcript to a file or GitHub issue.