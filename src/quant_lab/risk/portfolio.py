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
    requested_target_contracts: float
    final_target_contracts: float
    factor_score: float | None
    factor_multiplier: float | None
    applied_scale: float
    blocked: bool
    reasons: list[str]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class PortfolioRiskAllocation:
    symbol: str
    desired_side: int
    priority_score: float
    requested_risk_fraction: float
    allocated_risk_fraction: float
    allocation_scale: float
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


def portfolio_priority_score(*, strategy_score: float | None, factor_score: float | None) -> float:
    if strategy_score is None:
        strategy_strength = 0.0
    else:
        score_scale = 100.0 if strategy_score > 50.0 else 50.0
        strategy_strength = max(0.0, min(1.0, strategy_score / score_scale))
    factor_strength = 0.5 if factor_score is None else max(0.0, min(1.0, factor_score))
    return round((strategy_strength * 0.4) + (factor_strength * 0.6), 6)


def allocate_portfolio_risk_budgets(
    *,
    requests: dict[str, dict[str, float | int | None]],
    portfolio_max_total_risk: float,
    portfolio_max_same_direction_risk: float,
) -> dict[str, PortfolioRiskAllocation]:
    allocations: dict[str, PortfolioRiskAllocation] = {}
    active_requests: list[dict[str, float | int | str]] = []

    for symbol, request in requests.items():
        desired_side = int(request.get("desired_side") or 0)
        requested_risk_fraction = max(0.0, float(request.get("requested_risk_fraction") or 0.0))
        priority_score = max(0.0, float(request.get("priority_score") or 0.0))
        allocations[symbol] = PortfolioRiskAllocation(
            symbol=symbol,
            desired_side=desired_side,
            priority_score=round(priority_score, 6),
            requested_risk_fraction=round(requested_risk_fraction, 6),
            allocated_risk_fraction=round(requested_risk_fraction, 6),
            allocation_scale=1.0,
            blocked=False,
            reasons=[],
        )
        if desired_side != 0 and requested_risk_fraction > 0:
            active_requests.append(
                {
                    "symbol": symbol,
                    "side": desired_side,
                    "risk_fraction": requested_risk_fraction,
                    "priority_score": max(priority_score, 0.05),
                }
            )

    if not active_requests:
        return allocations

    same_side_caps = {1: portfolio_max_same_direction_risk, -1: portfolio_max_same_direction_risk}
    for side in (1, -1):
        group = [item for item in active_requests if int(item["side"]) == side]
        if group:
            _apply_risk_cap_to_requests(group, allocations, cap_fraction=same_side_caps[side], label="same-direction")

    refreshed_total = sum(float(item["risk_fraction"]) for item in active_requests)
    if refreshed_total > portfolio_max_total_risk:
        _apply_risk_cap_to_requests(
            active_requests,
            allocations,
            cap_fraction=portfolio_max_total_risk,
            label="portfolio-total",
        )

    for allocation in allocations.values():
        allocation.allocated_risk_fraction = round(allocation.allocated_risk_fraction, 6)
        allocation.allocation_scale = round(allocation.allocation_scale, 6)
    return allocations


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
            requested_target_contracts=float(plan.target_contracts),
            final_target_contracts=float(plan.target_contracts),
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
            requested_target_contracts=float(plan.target_contracts),
            final_target_contracts=float(plan.target_contracts),
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
            requested_target_contracts=float(plan.target_contracts),
            final_target_contracts=float(plan.target_contracts),
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
        requested_target_contracts=float(plan.target_contracts),
        final_target_contracts=float(plan.target_contracts),
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
    requests: dict[str, dict[str, float | int | None]] = {}
    original_targets: dict[str, float] = {}

    for symbol, state in symbol_states.items():
        plan: OrderPlan = state["plan"]
        instrument_config = state["instrument_config"]
        factor_snapshot = state.get("public_factor_snapshot")
        signal = state["signal"]
        original_targets[symbol] = float(plan.target_contracts)

        base_risk = estimate_plan_risk_fraction(
            plan=plan,
            contract_value=instrument_config.contract_value,
            equity_reference=max(total_equity, 1e-9),
        )
        priority_score = portfolio_priority_score(
            strategy_score=getattr(signal, "strategy_score", None),
            factor_score=factor_snapshot.score if isinstance(factor_snapshot, PublicFactorSnapshot) else None,
        )
        requests[symbol] = {
            "desired_side": plan.desired_side,
            "priority_score": priority_score,
            "requested_risk_fraction": base_risk,
        }
        decisions[symbol] = PortfolioRiskDecision(
            symbol=symbol,
            desired_side=plan.desired_side,
            priority_score=priority_score,
            base_risk_fraction=round(base_risk, 6),
            scaled_risk_fraction=round(base_risk, 6),
            requested_target_contracts=float(plan.target_contracts),
            final_target_contracts=float(plan.target_contracts),
            factor_score=factor_snapshot.score if isinstance(factor_snapshot, PublicFactorSnapshot) else None,
            factor_multiplier=(
                factor_snapshot.risk_multiplier if isinstance(factor_snapshot, PublicFactorSnapshot) else None
            ),
            applied_scale=1.0,
            blocked=False,
            reasons=[],
        )

    allocations = allocate_portfolio_risk_budgets(
        requests=requests,
        portfolio_max_total_risk=portfolio_max_total_risk,
        portfolio_max_same_direction_risk=portfolio_max_same_direction_risk,
    )

    for symbol, state in symbol_states.items():
        allocation = allocations[symbol]
        decision = decisions[symbol]
        plan: OrderPlan = state["plan"]
        instrument_config = state["instrument_config"]
        original_target = original_targets[symbol]
        if (
            original_target > 0
            and _has_open_instruction(plan)
            and allocation.allocation_scale < 0.999999
        ):
            scaled_target = original_target * allocation.allocation_scale
            _scale_plan_open_instruction(plan, target_contracts=scaled_target, lot_size=instrument_config.lot_size)
        for reason in allocation.reasons:
            decision.reasons.append(reason)
            if reason not in plan.warnings:
                plan.warnings.append(reason)
        actual_scale = allocation.allocation_scale
        if original_target > 0:
            actual_scale = max(0.0, float(plan.target_contracts) / original_target)
        decision.applied_scale = round(actual_scale, 6)
        decision.blocked = allocation.blocked or (original_target > 0 and plan.target_contracts <= 0)

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
        decisions[symbol].final_target_contracts = float(state["plan"].target_contracts)
    return decisions


def _apply_risk_cap_to_requests(
    group: list[dict[str, float | int | str]],
    allocations: dict[str, PortfolioRiskAllocation],
    *,
    cap_fraction: float,
    label: str,
) -> None:
    current_total = sum(item["risk_fraction"] for item in group)
    if current_total <= cap_fraction:
        return

    remaining_cap = max(0.0, float(cap_fraction))
    remaining_symbols = {
        str(item["symbol"]): {
            "priority_score": max(float(item["priority_score"]), 0.0),
            "max_fraction": max(float(item["risk_fraction"]), 0.0),
        }
        for item in group
        if float(item["risk_fraction"]) > 0
    }
    allocated_by_symbol = {symbol: 0.0 for symbol in remaining_symbols}

    while remaining_symbols and remaining_cap > 1e-12:
        total_priority = sum(entry["priority_score"] for entry in remaining_symbols.values())
        if total_priority <= 0:
            total_priority = float(len(remaining_symbols))

        saturated_symbols: list[str] = []
        provisional: dict[str, float] = {}
        for symbol, entry in remaining_symbols.items():
            share = remaining_cap * (
                (entry["priority_score"] / total_priority) if total_priority > 0 else (1.0 / len(remaining_symbols))
            )
            if share >= entry["max_fraction"] - 1e-12:
                allocated_by_symbol[symbol] += entry["max_fraction"]
                remaining_cap -= entry["max_fraction"]
                saturated_symbols.append(symbol)
            else:
                provisional[symbol] = share

        if not saturated_symbols:
            for symbol, share in provisional.items():
                allocated_by_symbol[symbol] += share
            remaining_cap = 0.0
            break

        for symbol in saturated_symbols:
            remaining_symbols.pop(symbol, None)

    for item in group:
        symbol = str(item["symbol"])
        current_fraction = max(float(item["risk_fraction"]), 0.0)
        allocated_fraction = max(0.0, allocated_by_symbol.get(symbol, 0.0))
        applied_scale = 0.0 if current_fraction <= 0 else min(1.0, allocated_fraction / current_fraction)
        item["risk_fraction"] = allocated_fraction

        allocation = allocations[symbol]
        allocation.allocated_risk_fraction = float(allocated_fraction)
        allocation.allocation_scale = round(allocation.allocation_scale * applied_scale, 6)
        if allocation.allocated_risk_fraction <= 0:
            allocation.blocked = True
        if applied_scale < 0.999999:
            reason = (
                f"{label} risk cap scaled target by {applied_scale:.2f} "
                f"(priority={item['priority_score']:.3f}, cap={cap_fraction:.4f})."
            )
            allocation.reasons.append(reason)


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
