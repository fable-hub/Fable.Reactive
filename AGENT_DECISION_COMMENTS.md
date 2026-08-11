# Agent Decision Comments

> Preserve the why in agent-written, human-reviewed code.

- Specification version: **0.1.1**
- Canonical repository: <https://github.com/dbrattli/adc>

Agent Decision Comments (ADCs) are concise, structured annotations that keep durable
engineering decisions beside the code they govern. They give human reviewers a
clear statement of intent and let future coding agents inherit the constraints
and context behind an implementation.

ADCs are source-level decision records: smaller and more local than an RFC or
ADR, but written with the same goal of preserving rationale.

ADCs are not transcripts of an agent's private reasoning. They record only the
engineering rationale that future contributors need.

---

## Quick start

To adopt ADCs in another repository, copy the convention from a published
release into that repository as `AGENT_DECISION_COMMENTS.md`. Then reference
the local file from `AGENTS.md`, `CLAUDE.md`, or the equivalent:

```text
This repository uses Agent Decision Comments.
See AGENT_DECISION_COMMENTS.md for the locally adopted convention.
Upstream releases: https://github.com/dbrattli/adc/releases

Before modifying code, read all active Agent Decision Comments in the affected
scope. Preserve them or update them explicitly. Add comments for non-obvious
decisions, invariants, assumptions, and tradeoffs introduced by your change.
```

If you use another stable path, such as `docs/agent-decision-comments.md`, name
that exact path in the instruction file. Prefer a descriptive filename over
`ADC.md` so contributors and agents can discover the convention easily.

Keep the local copy pinned to a release and update it through an ordinary code
review. Do not use a moving branch such as `main` as the sole source of agent
instructions; its contents can change without a corresponding repository
change.

An Agent Decision Comment looks like this:

```python
def process_events(queue):
    """
    Deliver queued events to their registered handlers.

    decision: processes events on one consumer to preserve arrival order
    invariant: handlers observe events in enqueue order
    tradeoff: limits throughput to gain deterministic processing
    """
```

The annotations use ordinary comments, documentation comments, or docstrings.
In documentation comments, they supplement normal prose rather than replacing
it. They require no runtime dependency and remain versioned with the code they
describe.

---

## The vocabulary

ADCs define four labels:

```text
decision:    A deliberate choice and the reason for it.
invariant:   A falsifiable property that the code must preserve.
assumption:  An external belief that this code does not guarantee.
tradeoff:    A cost accepted in exchange for a benefit.
```

Start with `decision:` and `invariant:`. Add `assumption:` or `tradeoff:` when
they expose information that would otherwise be easy to miss.

### `decision:`

Records the selected approach and why it is appropriate here. It does not imply
that every possible alternative was evaluated.

```text
decision: uses a fold instead of recursive descent to keep stack usage constant
```

### `invariant:`

Records a property that must remain true across future changes. It should be
specific enough for a reviewer to challenge and, where practical, for a test or
tool to verify.

```text
invariant: blocks accumulate in reverse order and are reversed exactly once
```

### `assumption:`

Records something outside the annotated code that is believed to be true but
is not guaranteed by that code. Include the consequence when it is useful.

```text
assumption: upstream preserves source-line order — otherwise block positions are invalid
```

A caller obligation is a precondition, not an assumption. Express preconditions
through types, contracts, validation, or the language's normal API documentation.

### `tradeoff:`

Records both the accepted cost and the resulting benefit.

```text
tradeoff: performs an O(n) copy per update to enable time-travel debugging
```

---

## Format

Use this general form:

```text
<label>: <statement> [— <reason or consequence>]
```

Follow these rules:

- Write one directive per physical line.
- Use present tense: `uses a fold`, not `was changed to use a fold`.
- Keep directives concise and within the repository's normal line length when
  practical.
- Place directives in the nearest comment or docstring attached to the code
  they govern.
- In a docstring or documentation comment, describe the construct in normal
  prose first, then leave a blank line before the directives.
- Use an ordinary comment when ADCs are needed but API documentation is not.
- State information that is not already evident from code, types, or tests.
- Prefer specific and falsifiable statements over general claims.
- Update the directive whenever its governed code or rationale changes.

The em dash is a readability convention, not a required parser delimiter.

### Good

```python
def process_events(queue):
    """
    Deliver queued events to their registered handlers.

    decision: uses one consumer because event ordering is externally observable
    invariant: the queue is drained before this function returns
    tradeoff: limits throughput to preserve deterministic processing
    """
```

```typescript
/**
 * Return the next application state for an action.
 *
 * decision: updates state immutably to support time-travel debugging
 * invariant: state.version increases monotonically and never resets
 * tradeoff: copies state on each update to retain previous versions
 */
function reducer(state: State, action: Action): State {
  // ...
}
```

The convention is not tied to a particular language or comment syntax. For
example, the same directives work in F# documentation comments:

```fsharp
(**
Parses source lines into a document.

decision: folds over the input to keep stack usage constant for files over 10k lines
invariant: blocks accumulate in reverse order and are reversed exactly once at the end
*)
let parse (lines: string seq) : Document =
    lines |> Seq.fold parseLine initial |> flushState |> _.Blocks |> List.rev
```

### Bad

```python
# decision: loops over the events
for event in events:
    process(event)
```

The annotation merely repeats the code.

```typescript
/* decision: uses immutable state because it is better */
function reducer(state: State, action: Action): State {
  // ...
}
```

The decision is vague and cannot be meaningfully reviewed.

### Public APIs and generated documentation

Public APIs may carry ADCs when their shape or semantics embody a non-obvious
architectural choice. Use them selectively for decisions such as portability
boundaries, lifecycle semantics, compatibility constraints, or intentional
escape hatches.

Anything callers must rely on remains stated in ordinary API documentation and,
where practical, enforced by tests. An ADC explains why the contract or API
shape was chosen; it does not replace consumer-facing documentation. If an
`invariant:` describes a public guarantee, state that guarantee in normal prose
first.

ADCs in documentation comments may appear in generated API documentation. Use
documentation comments when the rationale helps consumers understand the API.
Keep implementation-only rationale in ordinary source comments.

---

## When to write a comment

Write or update an ADC when a change:

- Chooses one meaningful algorithm, architecture, or data structure over another.
- Introduces ordering, consistency, concurrency, or lifecycle constraints.
- Establishes a boundary that future code must not accidentally cross.
- Relies on an external condition that could prove false.
- Accepts a known limitation in return for a concrete benefit.
- Implements behavior that a competent reviewer would reasonably question.
- Distills a relevant RFC or ADR decision at the point where it constrains code.

Do not add a comment for:

- Mechanics that are clear from the implementation.
- Facts already enforced by the type system.
- Trivial formatting, renaming, or mechanical refactoring.
- Generic advice that does not constrain the annotated code.
- Speculative reasoning that did not affect the implementation.
- Every function merely for consistency.

The goal is durable signal, not comment coverage.

---

## Scope

ADCs follow the structure of the source code:

```text
File
  Module or namespace
    Type
      Function or method
        Local block
```

A directive governs the construct it is attached to and its nested constructs.
Before changing code, collect the active directives from the file level down to
the specific site being modified.

Language conventions determine attachment:

- A file-level directive appears before the first declaration.
- A comment block immediately preceding a declaration governs that declaration.
- A Python docstring governs its containing module, class, or function.
- A local comment immediately preceding a block governs that block.

Directives from enclosing scopes accumulate. A narrower directive may clarify
or specialize a broader decision, but it does not silently cancel an invariant.
If active directives conflict, surface the conflict before changing the code.

### Explicit local departure

When a local implementation departs from a broader decision, acknowledge the
departure and contain its effects:

```typescript
/**
 * Parse source lines into document blocks.
 *
 * decision: uses immutable state by default so parser stages remain independently testable
 */
class Parser {
  /**
   * Parse source lines on an allocation-sensitive hot path.
   *
   * decision: mutates a local accumulator despite the class default — profiling shows a 10x gain
   * invariant: mutation remains inside this method and never escapes to the caller
   * tradeoff: gives up local immutability to reduce allocation on the parsing hot path
   */
  parseHot(lines: string[]): Block[] {
    // ...
  }
}
```

---

## Existing comments are active constraints

Before modifying code, read the ADCs already governing it.

- Preserve an `invariant:` or change it explicitly.
- Do not silently reverse a `decision:`.
- Validate an `assumption:` when the change depends on it.
- Reconsider a `tradeoff:` when its cost or benefit changes.

Annotations are not immutable historical artifacts. They describe the current
rationale. If a decision changes, update or remove the annotation in the same
change and call out the reversal in the review summary.

Leaving an obsolete directive in place is worse than removing it because it
creates false confidence for reviewers and future agents.

---

## Relationship to RFCs and ADRs

ADCs complement full decision records; they do not replace them.

Use an RFC or ADR when a decision:

- Spans several systems or repositories.
- Needs alternatives, consequences, status, ownership, or historical context.
- Requires discussion and approval outside the code review.
- Is important independently of a particular implementation site.

Once accepted, project the durable consequence into the relevant source code:

```typescript
/*
 * decision: stores monetary values as integer minor units — preserves ADR-004 rounding semantics
 * invariant: persisted amounts never use binary floating-point representation
 */
```

The full record preserves the discussion. The code-local comment preserves the
constraint where an agent is most likely to encounter it.

---

## Review workflow

ADCs make intent reviewable before implementation details. A reviewer can:

1. Read the active directives.
2. Agree with or challenge each decision.
3. Check that every invariant is upheld.
4. Probe assumptions against their external context.
5. Decide whether each tradeoff remains acceptable.
6. Review the implementation and tests against that stated intent.

The comments improve code review; they do not replace implementation review.

For teams that want a strict review contract, add this policy to their agent
instructions:

```text
A change that introduces a non-obvious engineering decision or constraint is
incomplete until its Agent Decision Comments are present and consistent with
the implementation. Reviewers evaluate the comments before reviewing the code.
```

---

## Long-form explanations

Documentation comments normally need only a brief summary before the
directives. When longer explanatory prose is useful, use the directives to
crystallize its durable result. This also applies to notebooks and literate
programs:

```python
def parse(lines):
    """
    The parser accumulates blocks using a fold rather than building a recursive
    call stack. Production documents frequently exceed 5,000 lines, and the
    recursive prototype overflowed on larger inputs.

    decision: folds over input instead of recursing to keep stack usage constant
    invariant: stack depth remains O(1) regardless of input length
    """
```

The prose explains the journey. The directives preserve the decision and its
constraint for reviewers, tools, and future agents.

---

## Versioning

The ADC specification follows Semantic Versioning. Patch releases clarify
wording or examples without changing meaning. Minor releases add compatible
directives or guidance. Major releases change existing directive semantics or
adoption requirements.

Version changes apply to the convention itself, not repository-only maintenance
such as CI or contributor documentation. Each specification version is
published as a GitHub release and `vX.Y.Z` tag so adopting repositories can
trace and review updates.

---

## The three layers

```text
Types      constrain data and interfaces
Tests      check observable behaviour
Comments   preserve decisions and constraints
```

All three describe different aspects of the system. Agent Decision Comments
capture intent that implementation and tests often cannot express on their own.

---

## Ecosystem

```text
Conventional Commits    structure intent in change history
Conventional Comments   structure intent in review feedback
Agent Decision Comments structure intent in agent-written source code
```

ADCs apply the same basic discipline—a small vocabulary, predictable form, and
human-readable meaning—to the decisions embedded in source code.
