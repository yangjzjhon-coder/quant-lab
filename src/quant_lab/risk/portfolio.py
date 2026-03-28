from __future__ import annotations

from dataclasses import asdict, dataclass
from math import floor
from typing import Any

from quant_lab.data.public_factors import PublicFactorSnapshot
from quant_lab.execution.planner import OrderInstruction, OrderPlan


@dataclass
class PortfolioRiskDecision:
    symbol: str
    desired_side: int
    priority_score: float
    base_risk_fraction: float
    scaled_risk_fraction: float
    factor_score: float | None
    factor_multiplier: float | None
    applied_scale: float
    blocked: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def estimate_plan_risk_fraction(
    *,
    plan: OrderPlan,
    contract_value: float,
    equity_reference: float,
) -> float:
    if (
        plan.target_contracts <= 0
        or plan.entry_price_estimate is None
        or plan.stop_price is None
        or equity_reference <= 0
    ):
        return 0.0
    risk_cash = abs(plan.entry_price_estimate - plan.stop_price) * plan.target_contracts * contract_value
    return max(0.0, float(risk_cash / equity_reference))


def apply_factor_overlay_to_plan(
    *,
    symbol: str,
    plan: OrderPlan,
    lot_size: float,
    factor_snapshot: PublicFactorSnapshot | None,
    min_factor_score: float,
) -> PortfolioRiskDecision:
    factor_score = factor_snapshot.score if factor_snapshot is not None else None
    factor_multiplier = factor_snapshot.risk_multiplier if factor_snapshot is not None else None
    reasons: list[str] = []

    if factor_snapshot is None or plan.target_contracts <= 0 or not _has_open_instruction(plan):
        return PortfolioRiskDecision(
            symbol=symbol,
            desired_side=plan.desired_side,
            priority_score=0.0,
            base_risk_fraction=0.0,
            scaled_risk_fraction=0.0,
            factor_score=factor_score,
            factor_multiplier=factor_multiplier,
            applied_scale=1.0,
            blocked=False,
            reasons=reasons,
        )

    score = factor_snapshot.score
    if score < min_factor_score:
        _scale_plan_open_instruction(plan, target_contracts=0.0, lot_size=lot_size)
        reason = (
            f"Public factor score {score:.3f} is below min_public_factor_score={min_factor_score:.3f}; "
            "new exposure is blocked."
        )
        plan.warnings.append(reason)
        reasons.append(reason)
        return PortfolioRiskDecision(
            symbol=symbol,
            desired_side=plan.desired_side,
            priority_score=score,
            base_risk_fraction=0.0,
            scaled_risk_fraction=0.0,
            factor_score=factor_score,
            factor_multiplier=factor_multiplier,
            applied_scale=0.0,
            blocked=True,
            reasons=reasons,
        )

    if factor_snapshot.risk_multiplier >= 0.999:
        return PortfolioRiskDecision(
            symbol=symbol,
            desired_side=plan.desired_side,
            priority_score=score,
            base_risk_fraction=0.0,
            scaled_risk_fraction=0.0,
            factor_score=factor_score,
            factor_multiplier=factor_multiplier,
            applied_scale=1.0,
            blocked=False,
            reasons=reasons,
        )

    scaled_contracts = _round_down_to_lot(plan.target_contracts * factor_snapshot.risk_multiplier, lot_size)
    applied_scale = 0.0 if plan.target_contracts <= 0 else (scaled_contracts / plan.target_contracts)
    _scale_plan_open_instruction(plan, target_contracts=scaled_contracts, lot_size=lot_size)
    reason = (
        f"Public factor overlay scaled target by {applied_scale:.2f} "
        f"(score={score:.3f}, multiplier={factor_snapshot.risk_multiplier:.3f})."
    )
    plan.warnings.append(reason)
    reasons.append(reason)
    return PortfolioRiskDecision(
        symbol=symbol,
        desired_side=plan.desired_side,
        priority_score=score,
        base_risk_fraction=0.0,
        scaled_risk_fraction=0.0,
        factor_score=factor_score,
        factor_multiplier=factor_multiplier,
        applied_scale=applied_scale,
        blocked=scaled_contracts <= 0,
        reasons=reasons,
    )


def apply_portfolio_risk_caps(
    *,
    symbol_states: dict[str, dict[str, Any]],
    total_equity: float,
    portfolio_max_total_risk: float,
    portfolio_max_same_direction_risk: float,
) -> dict[str, PortfolioRiskDecision]:
    decisions: dict[str, PortfolioRiskDecision] = {}
    candidates: list[dict[str, Any]] = []

    for symbol, state in symbol_states.items():
        plan: OrderPlan = state["plan"]
        instrument_config = state["instrument_config"]
        factor_snapshot = state.get("public_factor_snapshot")
        signal = state["signal"]

        base_risk = estimate_plan_risk_fraction(
            plan=plan,
            contract_value=instrument_config.contract_value,
            equity_reference=max(total_equity, 1e-9),
        )
        priority_score = _priority_score(
            strategy_score=getattr(signal, "strategy_score", None),
            factor_score=factor_snapshot.score if isinstance(factor_snapshot, PublicFactorSnapshot) else None,
        )
        decision = PortfolioRiskDecision(
            symbol=symbol,
            desired_side=plan.desired_side,
            priority_score=priority_score,
            base_risk_fraction=round(base_risk, 6),
            scaled_risk_fraction=round(base_risk, 6),
            factor_score=factor_snapshot.score if isinstance(factor_snapshot, PublicFactorSnapshot) else None,
            factor_multiplier=(
                factor_snapshot.risk_multiplier if isinstance(factor_snapshot, PublicFactorSnapshot) else None
            ),
            applied_scale=1.0,
            blocked=False,
            reasons=[],
        )
        decisions[symbol] = decision

        if plan.target_contracts > 0 and _has_open_instruction(plan) and base_risk > 0 and plan.desired_side != 0:
            candidates.append(
                {
                    "symbol": symbol,
                    "side": plan.desired_side,
                    "risk_fraction": base_risk,
                    "priority_score": max(priority_score, 0.05),
                    "plan": plan,
                    "lot_size": instrument_config.lot_size,
                }
            )

    if not candidates or total_equity <= 0:
        return decisions

    same_side_caps = {1: portfolio_max_same_direction_risk, -1: portfolio_max_same_direction_risk}
    for side in (1, -1):
        group = [item for item in candidates if item["side"] == side]
        if not group:
            continue
        _apply_group_cap(group, same_side_caps[side], decisions, label="same-direction")

    refreshed_total = sum(item["risk_fraction"] for item in candidates)
    if refreshed_total > portfolio_max_total_risk:
        _apply_group_cap(candidates, portfolio_max_total_risk, decisions, label="portfolio-total")

    for symbol, state in symbol_states.items():
        instrument_config = state["instrument_config"]
        decisions[symbol].scaled_risk_fraction = round(
            estimate_plan_risk_fraction(
                plan=state["plan"],
                contract_value=instrument_config.contract_value,
                equity_reference=max(total_equity, 1e-9),
            ),
            6,
        )
    return decisions


def _apply_group_cap(
    group: list[dict[str, Any]],
    cap_fraction: float,
    decisions: dict[str, PortfolioRiskDecision],
    *,
    label: str,
) -> None:
    current_total = sum(item["risk_fraction"] for item in group)
    if current_total <= cap_fraction:
        return

    total_priority = sum(item["priority_score"] for item in group)
    if total_priority <= 0:
        total_priority = float(len(group))

    for item in sorted(group, key=lambda row: row["priority_score"], reverse=True):
        alloc_fraction = cap_fraction * (item["priority_score"] / total_priority)
        current_fraction = item["risk_fraction"]
        if current_fraction <= 0:
            continue
        scale = min(1.0, alloc_fraction / current_fraction)
        scaled_contracts = _round_down_to_lot(item["plan"].target_contracts * scale, item["lot_size"])
        applied_scale = 0.0 if item["plan"].target_contracts <= 0 else (scaled_contracts / item["plan"].target_contracts)
        _scale_plan_open_instruction(item["plan"], target_contracts=scaled_contracts, lot_size=item["lot_size"])
        item["risk_fraction"] = current_fraction * applied_scale

        decision = decisions[item["symbol"]]
        decision.applied_scale = round(decision.applied_scale * applied_scale, 6)
        if scaled_contracts <= 0:
            decision.blocked = True
        reason = (
            f"{label} risk cap scaled target by {applied_scale:.2f} "
            f"(priority={item['priority_score']:.3f}, cap={cap_fraction:.4f})."
        )
        decision.reasons.append(reason)
        item["plan"].warnings.append(reason)


def _scale_plan_open_instruction(plan: OrderPlan, *, target_contracts: float, lot_size: float) -> None:
    rounded_target = _round_down_to_lot(target_contracts, lot_size)
    plan.target_contracts = rounded_target

    retained_instructions: list[OrderInstruction] = []
    had_close_instruction = False
    for instruction in plan.instructions:
        if instruction.purpose == "open_target":
            if rounded_target > 0:
                instruction.size = rounded_target
                retained_instructions.append(instruction)
        else:
            had_close_instruction = True
            retained_instructions.append(instruction)
    plan.instructions = retained_instructions

    if rounded_target <= 0:
        if had_close_instruction:
            plan.action = "close"
            plan.reason = f"{plan.reason} Portfolio risk removed replacement entry."
        else:
            plan.action = "hold"
            plan.reason = "Portfolio risk blocked new exposure."


def _has_open_instruction(plan: OrderPlan) -> bool:
    return any(instruction.purpose == "open_target" for instruction in plan.instructions)


def _round_down_to_lot(value: float, lot_size: float) -> float:
    if lot_size <= 0:
        return max(0.0, value)
    steps = floor(max(0.0, value) / lot_size)
    return steps * lot_size


def _priority_score(*, strategy_score: float | None, factor_score: float | None) -> float:
    if strategy_score is None:
        strategy_strength = 0.0
    else:
        score_scale = 100.0 if strategy_score > 50.0 else 50.0
        strategy_strength = max(0.0, min(1.0, strategy_score / score_scale))
    factor_strength = 0.5 if factor_score is None else max(0.0, min(1.0, factor_score))
    return round((strategy_strength * 0.4) + (factor_strength * 0.6), 6)
