# Quant-Lab Runtime Hard Constraints

## Purpose

These constraints are project rules, not suggestions. They exist to keep future live-trading work on the same shared runtime spine and to avoid reopening the split paths we just closed.

## Constraint 1

`ready / blocked / halt / duplicate / reconcile` must come from one shared decision source.

Implementation rule:

- Shared runtime logic computes the decision.
- CLI, service API, dashboard, and worker only render or adapt that decision.
- Do not re-implement the same status logic in multiple entrypoints.

Current shared policy source:

- [`src/quant_lab/application/runtime_policy.py`](/E:/quant-lab/src/quant_lab/application/runtime_policy.py)

## Constraint 2

Any new live-trading capability must be implemented as a shared runtime/helper first, and only then wired into CLI, service, or worker entrypoints.

Implementation rule:

- Add reusable helpers under `src/quant_lab/application/` or another shared runtime module.
- Only after that, connect the helper to `cli.py`, `service/*.py`, or a future execution worker.
- Do not create a CLI-only live path or a service-only live path.

## Constraint 3

`single` and `portfolio`, `demo` and `live`, must be parameterized modes, not parallel implementations.

Implementation rule:

- Mode differences are allowed in data payloads and execution guards.
- Mode differences are not allowed as separate business-rule stacks.
- If two modes need the same rule, the rule belongs in shared runtime code with a mode parameter.

## Stage 0 Interpretation

For the first live rollout, the system is frozen to:

- 1 account profile
- 1 symbol
- 1 approved candidate
- 1 signal/execution bar combination

That freeze is now represented by the shared rollout policy payload surfaced through runtime preflight.
