# Quant-Lab Stage 0 Live Rollout Runbook

## Goal

Stage 0 does not mean "turn on live trading immediately". It means we freeze the live rollout surface so the first 7x24 rollout has the smallest possible blast radius:

- 1 account
- 1 symbol
- 1 approved candidate
- 1 signal/execution bar combination

Portfolio research and demo flows stay available. The live rollout stays single-symbol.

## Files

- Live rollout config template:
  - [`config/live_single_btc.example.yaml`](/E:/quant-lab/config/live_single_btc.example.yaml)
- OKX profile template:
  - [`config/okx_profiles.example.toml`](/E:/quant-lab/config/okx_profiles.example.toml)
- Shared hard constraints:
  - [`docs/runtime_hard_constraints_2026-03-30.md`](/E:/quant-lab/docs/runtime_hard_constraints_2026-03-30.md)

## Required Config Shape

The live config must satisfy all of the following:

- `okx.use_demo: false`
- `okx.profile` must match the frozen live account profile
- `portfolio.symbols` resolves to exactly one symbol
- `trading.require_approved_candidate: true`
- `trading.strategy_router_enabled: false`
- `trading.execution_candidate_id` is pinned to exactly one candidate
- `strategy.signal_bar` is pinned
- `strategy.execution_bar` is pinned
- `rollout.phase: live_single`

## Unique Candidate Binding Rule

The Stage 0 live rollout allows exactly one execution candidate.

Required rule:

- `trading.execution_candidate_id` must equal `rollout.required_candidate_id`
- `trading.execution_candidate_name` should equal `rollout.required_candidate_name`
- `trading.execution_candidate_map` must stay empty
- `trading.strategy_router_enabled` must stay `false`

Interpretation:

- Stage 0 live rollout uses a single approved candidate binding.
- Router-based live execution is explicitly out of scope for this stage.

## Account Profile Rule

The frozen live account is identified by `okx.profile`.

Required rule:

- `okx.profile` must equal `rollout.account_profile`
- The matching profile in `config.toml` must have `demo = false`

This avoids a config that points at the wrong account while looking otherwise valid.

## Pre-Launch Checklist

1. Copy the live template config and fill in only the frozen Stage 0 values.
2. Create the matching OKX profile in your local `config.toml`.
3. Keep `trading.allow_order_placement: false` during dry-run validation.
4. Run runtime preflight and verify:
   - `runtime_policy.status == "ready"`
   - `rollout_policy.status == "ready"`
   - `demo_trading.ready == false` if you are still using dry-run mode
5. Verify the bound candidate is approved for the intended scope.
6. Verify the symbol, candidate id, signal bar, and execution bar exactly match the rollout lock.

## First Activation Procedure

1. Validate research artifacts and candidate approval.
2. Validate the account profile and network/proxy path.
3. Run preflight with `allow_order_placement: false`.
4. Flip only `trading.allow_order_placement` after preflight is clean.
5. Start with the smallest acceptable size and observe heartbeats, alerts, and reconcile state.

## Abort Conditions

Do not proceed if any of these is true:

- `runtime_policy.status != ready`
- `rollout_policy.status != ready`
- More than one symbol is configured
- Router mode is enabled
- Candidate id or candidate name does not match the rollout lock
- The config points to a demo profile or ambiguous account profile

## What Stage 0 Does Not Include

- Portfolio live execution
- Router-based live execution
- Multiple candidate pools
- Multiple live accounts
- Automatic strategy switching

Those belong to later phases after execution recovery and account-level risk controls are hardened.
