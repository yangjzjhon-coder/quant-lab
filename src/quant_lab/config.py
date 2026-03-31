from __future__ import annotations

import os
import subprocess
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field, field_validator, model_validator

from quant_lab.utils.timeframes import bar_to_timedelta


class OkxConfig(BaseModel):
    rest_base_url: str = "https://www.okx.com"
    profile: str | None = None
    config_file: Path | None = None
    api_key: str | None = None
    secret_key: str | None = None
    passphrase: str | None = None
    proxy_url: str | None = None
    use_demo: bool = False


class MarketDataConfig(BaseModel):
    provider: str = "okx"
    base_url: str | None = None
    proxy_url: str | None = None
    timeout_seconds: float = 20.0
    api_key: str | None = None
    extra_headers: dict[str, str] = Field(default_factory=dict)
    provider_options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider", mode="before")
    @classmethod
    def _normalize_provider(cls, value: Any) -> str:
        return _normalized_token(value)

    @model_validator(mode="after")
    def _validate_model(self) -> "MarketDataConfig":
        _require_positive("market_data.timeout_seconds", self.timeout_seconds)
        return self


class InstrumentConfig(BaseModel):
    symbol: str = "BTC-USDT-SWAP"
    instrument_type: str = "SWAP"
    contract_value: float = 0.01
    contract_value_currency: str = "BTC"
    lot_size: float = 1.0
    min_size: float = 1.0
    tick_size: float | None = None
    settle_currency: str | None = None

    @model_validator(mode="after")
    def _validate_model(self) -> "InstrumentConfig":
        _require_positive("instrument.contract_value", self.contract_value)
        _require_positive("instrument.lot_size", self.lot_size)
        _require_positive("instrument.min_size", self.min_size)
        if self.tick_size is not None:
            _require_positive("instrument.tick_size", self.tick_size)
        return self


class StrategyConfig(BaseModel):
    name: str = "ema_trend_4h"
    variant: str = "ema_cross"
    signal_bar: str = "4H"
    execution_bar: str = "1m"
    fast_ema: int = 20
    slow_ema: int = 50
    trend_ema: int = 200
    atr_period: int = 14
    atr_stop_multiple: float = 2.0
    adx_period: int = 14
    adx_threshold: float = 20.0
    allow_short: bool = False
    volume_window: int = 24
    min_volume_ratio: float = 1.15
    cmf_period: int = 20
    min_cmf_abs: float = 0.05
    min_body_atr_ratio: float = 0.2
    volatility_window: int = 72
    min_volatility_ratio: float = 0.8
    max_volatility_ratio: float = 2.5
    breakout_buffer_atr: float = 0.05
    retest_tolerance_atr: float = 0.35
    max_breakout_extension_atr: float = 1.6
    min_channel_width_atr: float = 1.0
    min_trend_score: float = 32.0
    trend_slope_window: int = 3
    min_trend_slope_atr: float = 0.05
    min_ema_spread_atr: float = 0.2
    use_public_factor_overlay: bool = True
    min_public_factor_score: float = 0.35

    @model_validator(mode="after")
    def _validate_model(self) -> "StrategyConfig":
        signal_delta = _require_bar("strategy.signal_bar", self.signal_bar)
        execution_delta = _require_bar("strategy.execution_bar", self.execution_bar)
        if execution_delta > signal_delta:
            raise ValueError(
                "strategy.execution_bar cannot be coarser than strategy.signal_bar"
            )
        _require_positive_int("strategy.fast_ema", self.fast_ema)
        _require_positive_int("strategy.slow_ema", self.slow_ema)
        _require_positive_int("strategy.trend_ema", self.trend_ema)
        if not (self.fast_ema < self.slow_ema < self.trend_ema):
            raise ValueError("strategy EMA windows must satisfy fast_ema < slow_ema < trend_ema")
        _require_positive_int("strategy.atr_period", self.atr_period)
        _require_positive("strategy.atr_stop_multiple", self.atr_stop_multiple)
        _require_positive_int("strategy.adx_period", self.adx_period)
        _require_non_negative("strategy.adx_threshold", self.adx_threshold)
        _require_positive_int("strategy.volume_window", self.volume_window)
        _require_positive("strategy.min_volume_ratio", self.min_volume_ratio)
        _require_positive_int("strategy.cmf_period", self.cmf_period)
        _require_non_negative("strategy.min_cmf_abs", self.min_cmf_abs)
        _require_non_negative("strategy.min_body_atr_ratio", self.min_body_atr_ratio)
        _require_positive_int("strategy.volatility_window", self.volatility_window)
        _require_positive("strategy.min_volatility_ratio", self.min_volatility_ratio)
        _require_positive("strategy.max_volatility_ratio", self.max_volatility_ratio)
        if self.max_volatility_ratio < self.min_volatility_ratio:
            raise ValueError(
                "strategy.max_volatility_ratio must be >= strategy.min_volatility_ratio"
            )
        _require_non_negative("strategy.breakout_buffer_atr", self.breakout_buffer_atr)
        _require_non_negative("strategy.retest_tolerance_atr", self.retest_tolerance_atr)
        _require_non_negative(
            "strategy.max_breakout_extension_atr", self.max_breakout_extension_atr
        )
        _require_positive("strategy.min_channel_width_atr", self.min_channel_width_atr)
        _require_non_negative("strategy.min_trend_score", self.min_trend_score)
        _require_positive_int("strategy.trend_slope_window", self.trend_slope_window)
        _require_non_negative("strategy.min_trend_slope_atr", self.min_trend_slope_atr)
        _require_non_negative("strategy.min_ema_spread_atr", self.min_ema_spread_atr)
        _require_fraction(
            "strategy.min_public_factor_score",
            self.min_public_factor_score,
            allow_zero=True,
        )
        return self


class ExecutionConfig(BaseModel):
    initial_equity: float = 10_000.0
    fee_bps: float = 5.0
    slippage_bps: float = 3.0
    latency_minutes: int = 1
    minimum_notional: float = 25.0
    max_leverage: float = 3.0
    max_bar_participation: float = 0.1
    market_impact_bps: float = 12.0
    excess_impact_bps: float = 18.0
    volatility_impact_share: float = 0.25
    funding_interval_hours: int = 8
    missing_funding_rate_bps: float = 1.0

    @model_validator(mode="after")
    def _validate_model(self) -> "ExecutionConfig":
        _require_positive("execution.initial_equity", self.initial_equity)
        _require_non_negative("execution.fee_bps", self.fee_bps)
        _require_non_negative("execution.slippage_bps", self.slippage_bps)
        _require_non_negative_int("execution.latency_minutes", self.latency_minutes)
        _require_positive("execution.minimum_notional", self.minimum_notional)
        _require_positive("execution.max_leverage", self.max_leverage)
        _require_fraction(
            "execution.max_bar_participation",
            self.max_bar_participation,
            allow_zero=False,
        )
        _require_non_negative("execution.market_impact_bps", self.market_impact_bps)
        _require_non_negative("execution.excess_impact_bps", self.excess_impact_bps)
        _require_fraction(
            "execution.volatility_impact_share",
            self.volatility_impact_share,
            allow_zero=True,
        )
        _require_positive_int("execution.funding_interval_hours", self.funding_interval_hours)
        _require_non_negative(
            "execution.missing_funding_rate_bps",
            self.missing_funding_rate_bps,
        )
        return self


class RiskConfig(BaseModel):
    risk_per_trade: float = 0.02
    weekly_drawdown_pause: float = 0.06
    portfolio_max_total_risk: float = 0.03
    portfolio_max_same_direction_risk: float = 0.025

    @model_validator(mode="after")
    def _validate_model(self) -> "RiskConfig":
        _require_fraction("risk.risk_per_trade", self.risk_per_trade, allow_zero=False)
        _require_fraction(
            "risk.weekly_drawdown_pause",
            self.weekly_drawdown_pause,
            allow_zero=False,
        )
        _require_fraction(
            "risk.portfolio_max_total_risk",
            self.portfolio_max_total_risk,
            allow_zero=False,
        )
        _require_fraction(
            "risk.portfolio_max_same_direction_risk",
            self.portfolio_max_same_direction_risk,
            allow_zero=False,
        )
        if self.portfolio_max_same_direction_risk > self.portfolio_max_total_risk:
            raise ValueError(
                "risk.portfolio_max_same_direction_risk must be <= risk.portfolio_max_total_risk"
            )
        if self.risk_per_trade > self.portfolio_max_total_risk:
            raise ValueError("risk.risk_per_trade must be <= risk.portfolio_max_total_risk")
        return self


class TradingConfig(BaseModel):
    td_mode: str = "cross"
    position_mode: str = "net_mode"
    order_type: str = "market"
    signal_lookback_bars: int = 240
    execution_lookback_bars: int = 720
    poll_interval_seconds: int = 60
    allow_order_placement: bool = False
    max_order_contracts: float | None = None
    attach_stop_loss_on_entry: bool = True
    stop_trigger_price_type: str = "mark"
    order_tag: str = "quantlab"
    require_approved_candidate: bool = False
    execution_candidate_id: int | None = None
    execution_candidate_name: str | None = None
    strategy_router_enabled: bool = False
    strategy_router_fallback_to_config: bool = True
    execution_candidate_map: dict[str, int] = Field(default_factory=dict)

    @field_validator("td_mode", "position_mode", "order_type", "stop_trigger_price_type", mode="before")
    @classmethod
    def _normalize_choice_fields(cls, value: Any) -> str:
        return _normalized_token(value)

    @field_validator("execution_candidate_map", mode="before")
    @classmethod
    def _normalize_candidate_map(cls, value: Any) -> dict[str, int]:
        if value is None or value == "":
            return {}
        if not isinstance(value, dict):
            raise ValueError("trading.execution_candidate_map must be a mapping")
        normalized: dict[str, int] = {}
        for raw_key, raw_candidate in value.items():
            route_key = str(raw_key or "").strip()
            if not route_key:
                raise ValueError("trading.execution_candidate_map cannot contain empty route keys")
            candidate_id = int(raw_candidate)
            if candidate_id <= 0:
                raise ValueError(
                    f"trading.execution_candidate_map[{route_key!r}] must be a positive integer"
                )
            normalized[route_key] = candidate_id
        return normalized

    @model_validator(mode="after")
    def _validate_model(self) -> "TradingConfig":
        _require_positive_int("trading.signal_lookback_bars", self.signal_lookback_bars)
        _require_positive_int("trading.execution_lookback_bars", self.execution_lookback_bars)
        _require_positive_int("trading.poll_interval_seconds", self.poll_interval_seconds)
        if self.max_order_contracts is not None:
            _require_positive("trading.max_order_contracts", self.max_order_contracts)
        if self.execution_candidate_id is not None and int(self.execution_candidate_id) <= 0:
            raise ValueError("trading.execution_candidate_id must be a positive integer")
        if self.td_mode not in {"cross", "isolated"}:
            raise ValueError("trading.td_mode must be one of: cross, isolated")
        if self.position_mode not in {"net_mode", "long_short_mode"}:
            raise ValueError("trading.position_mode must be one of: net_mode, long_short_mode")
        if self.order_type not in {"market", "limit"}:
            raise ValueError("trading.order_type must be one of: market, limit")
        if self.stop_trigger_price_type not in {"mark", "last", "index"}:
            raise ValueError("trading.stop_trigger_price_type must be one of: mark, last, index")
        if (
            self.strategy_router_enabled
            and not self.strategy_router_fallback_to_config
            and not self.execution_candidate_map
        ):
            raise ValueError(
                "trading.strategy_router_enabled=true with fallback disabled requires execution_candidate_map"
            )
        return self


class PortfolioConfig(BaseModel):
    symbols: list[str] = Field(default_factory=list)

    @field_validator("symbols", mode="before")
    @classmethod
    def _normalize_symbols(cls, value: Any) -> list[str]:
        if value is None or value == "":
            return []
        if not isinstance(value, list):
            raise ValueError("portfolio.symbols must be a list")
        normalized: list[str] = []
        for item in value:
            symbol = str(item or "").strip()
            if symbol and symbol not in normalized:
                normalized.append(symbol)
        return normalized


class StorageConfig(BaseModel):
    data_dir: Path = Path("data")
    raw_dir: Path = Path("data/raw")
    report_dir: Path = Path("data/reports")

    def resolved(self, base_dir: Path) -> "StorageConfig":
        return StorageConfig(
            data_dir=_resolve_path(base_dir, self.data_dir),
            raw_dir=_resolve_path(base_dir, self.raw_dir),
            report_dir=_resolve_path(base_dir, self.report_dir),
        )


class DatabaseConfig(BaseModel):
    url: str = "sqlite:///data/quant_lab.db"

    def resolved(self, base_dir: Path) -> "DatabaseConfig":
        return DatabaseConfig(url=_resolve_database_url(base_dir, self.url))


class ServiceConfig(BaseModel):
    host: str = "0.0.0.0"
    port: int = 18080
    heartbeat_interval_seconds: int = 60
    report_stale_minutes: int = 180

    @model_validator(mode="after")
    def _validate_model(self) -> "ServiceConfig":
        if not (1 <= int(self.port) <= 65535):
            raise ValueError("service.port must be between 1 and 65535")
        _require_positive_int(
            "service.heartbeat_interval_seconds",
            self.heartbeat_interval_seconds,
        )
        _require_positive_int("service.report_stale_minutes", self.report_stale_minutes)
        return self


class AlertsConfig(BaseModel):
    telegram_enabled: bool = False
    telegram_bot_token: str | None = None
    telegram_chat_id: str | None = None
    email_enabled: bool = False
    email_from: str | None = None
    email_to: list[str] = Field(default_factory=list)
    email_subject_prefix: str = "[quant-lab]"
    smtp_host: str | None = None
    smtp_port: int = 587
    smtp_username: str | None = None
    smtp_password: str | None = None
    smtp_use_tls: bool = True
    smtp_use_ssl: bool = False
    smtp_timeout_seconds: float = 20.0
    send_on_halt: bool = True
    send_on_recovery: bool = True
    send_on_report_stale: bool = True
    send_on_demo_submit: bool = True
    send_on_demo_error: bool = True
    demo_error_cooldown_seconds: int = 900

    @field_validator("email_to", mode="before")
    @classmethod
    def _normalize_email_to(cls, value: Any) -> list[str]:
        if value is None or value == "":
            return []
        if isinstance(value, str):
            value = [value]
        if not isinstance(value, list):
            raise ValueError("alerts.email_to must be a list of addresses")
        normalized: list[str] = []
        for item in value:
            address = str(item or "").strip()
            if address and address not in normalized:
                normalized.append(address)
        return normalized

    @model_validator(mode="after")
    def _validate_model(self) -> "AlertsConfig":
        _require_positive("alerts.smtp_timeout_seconds", self.smtp_timeout_seconds)
        _require_non_negative_int(
            "alerts.demo_error_cooldown_seconds",
            self.demo_error_cooldown_seconds,
        )
        if self.smtp_use_tls and self.smtp_use_ssl:
            raise ValueError("alerts.smtp_use_tls and alerts.smtp_use_ssl cannot both be true")
        return self


class ResearchAIConfig(BaseModel):
    enabled: bool = False
    provider: str = "disabled"
    base_url: str | None = None
    api_key: str | None = None
    model: str | None = None
    timeout_seconds: float = 60.0
    temperature: float = 0.2
    max_output_tokens: int | None = 2000
    max_retries: int = 1
    default_system_prompt: str | None = None
    role_models: dict[str, str] = Field(default_factory=dict)
    role_system_prompts: dict[str, str] = Field(default_factory=dict)
    extra_headers: dict[str, str] = Field(default_factory=dict)
    provider_options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider", mode="before")
    @classmethod
    def _normalize_provider(cls, value: Any) -> str:
        return _normalized_token(value)

    @model_validator(mode="after")
    def _validate_model(self) -> "ResearchAIConfig":
        _require_positive("research_ai.timeout_seconds", self.timeout_seconds)
        _require_non_negative_int("research_ai.max_retries", self.max_retries)
        _require_fraction("research_ai.temperature", self.temperature, allow_zero=True, upper_bound=2.0)
        if self.max_output_tokens is not None and int(self.max_output_tokens) <= 0:
            raise ValueError("research_ai.max_output_tokens must be a positive integer")
        return self


class ResearchAgentConfig(BaseModel):
    enabled: bool = False
    provider: str = "disabled"
    base_url: str | None = None
    api_key: str | None = None
    timeout_seconds: float = 90.0
    max_retries: int = 1
    workflow_path: str = "/run"
    probe_path: str = "/health"
    extra_headers: dict[str, str] = Field(default_factory=dict)
    local_repo_path: Path | None = None
    python_executable: str | None = None
    provider_options: dict[str, Any] = Field(default_factory=dict)

    @field_validator("provider", mode="before")
    @classmethod
    def _normalize_provider(cls, value: Any) -> str:
        return _normalized_token(value)

    @model_validator(mode="after")
    def _validate_model(self) -> "ResearchAgentConfig":
        _require_positive("research_agent.timeout_seconds", self.timeout_seconds)
        _require_non_negative_int("research_agent.max_retries", self.max_retries)
        return self


class RolloutConfig(BaseModel):
    phase: str = "research"
    account_profile: str | None = None
    allowed_symbol: str | None = None
    required_candidate_id: int | None = None
    required_candidate_name: str | None = None
    required_signal_bar: str | None = None
    required_execution_bar: str | None = None

    @field_validator("phase", mode="before")
    @classmethod
    def _normalize_phase(cls, value: Any) -> str:
        return _normalized_token(value)

    @field_validator("account_profile", "allowed_symbol", "required_candidate_name", mode="before")
    @classmethod
    def _normalize_optional_text(cls, value: Any) -> str | None:
        if value in {"", None}:
            return None
        normalized = str(value).strip()
        return normalized or None

    @field_validator("required_signal_bar", "required_execution_bar", mode="before")
    @classmethod
    def _normalize_optional_bar(cls, value: Any) -> str | None:
        if value in {"", None}:
            return None
        return str(value).strip()

    @model_validator(mode="after")
    def _validate_model(self) -> "RolloutConfig":
        if self.phase not in {"research", "demo", "live_single"}:
            raise ValueError("rollout.phase must be one of: research, demo, live_single")
        if self.required_candidate_id is not None and int(self.required_candidate_id) <= 0:
            raise ValueError("rollout.required_candidate_id must be a positive integer")
        if self.required_signal_bar is not None:
            _require_bar("rollout.required_signal_bar", self.required_signal_bar)
        if self.required_execution_bar is not None:
            _require_bar("rollout.required_execution_bar", self.required_execution_bar)
        if self.required_signal_bar and self.required_execution_bar:
            signal_delta = bar_to_timedelta(self.required_signal_bar)
            execution_delta = bar_to_timedelta(self.required_execution_bar)
            if execution_delta > signal_delta:
                raise ValueError(
                    "rollout.required_execution_bar cannot be coarser than rollout.required_signal_bar"
                )
        if self.phase == "live_single":
            if not self.account_profile:
                raise ValueError("rollout.account_profile is required when rollout.phase=live_single")
            if not self.allowed_symbol:
                raise ValueError("rollout.allowed_symbol is required when rollout.phase=live_single")
            if self.required_candidate_id is None:
                raise ValueError("rollout.required_candidate_id is required when rollout.phase=live_single")
            if not self.required_signal_bar:
                raise ValueError("rollout.required_signal_bar is required when rollout.phase=live_single")
            if not self.required_execution_bar:
                raise ValueError("rollout.required_execution_bar is required when rollout.phase=live_single")
        return self


class AppConfig(BaseModel):
    okx: OkxConfig = Field(default_factory=OkxConfig)
    market_data: MarketDataConfig = Field(default_factory=MarketDataConfig)
    instrument: InstrumentConfig = Field(default_factory=InstrumentConfig)
    portfolio: PortfolioConfig = Field(default_factory=PortfolioConfig)
    strategy: StrategyConfig = Field(default_factory=StrategyConfig)
    execution: ExecutionConfig = Field(default_factory=ExecutionConfig)
    risk: RiskConfig = Field(default_factory=RiskConfig)
    trading: TradingConfig = Field(default_factory=TradingConfig)
    storage: StorageConfig = Field(default_factory=StorageConfig)
    database: DatabaseConfig = Field(default_factory=DatabaseConfig)
    service: ServiceConfig = Field(default_factory=ServiceConfig)
    alerts: AlertsConfig = Field(default_factory=AlertsConfig)
    research_ai: ResearchAIConfig = Field(default_factory=ResearchAIConfig)
    research_agent: ResearchAgentConfig = Field(default_factory=ResearchAgentConfig)
    rollout: RolloutConfig = Field(default_factory=RolloutConfig)

    @model_validator(mode="after")
    def _validate_model(self) -> "AppConfig":
        if self.trading.position_mode == "long_short_mode" and self.trading.td_mode == "isolated":
            raise ValueError(
                "trading.position_mode=long_short_mode is not supported with trading.td_mode=isolated"
            )
        return self


def configured_symbols(config: AppConfig) -> list[str]:
    raw_symbols = config.portfolio.symbols or [config.instrument.symbol]
    symbols: list[str] = []
    for symbol in raw_symbols:
        normalized = str(symbol).strip()
        if normalized and normalized not in symbols:
            symbols.append(normalized)
    return symbols or [config.instrument.symbol]


def load_config(config_path: Path) -> AppConfig:
    project_env = config_path.resolve().parent.parent / ".env"
    load_dotenv(project_env)
    load_dotenv()
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    cfg = AppConfig.model_validate(raw)
    base_dir = config_path.resolve().parent.parent
    raw_okx = raw.get("okx") or {}

    okx_profile = _env_value("OKX_PROFILE") or cfg.okx.profile
    okx_config_file = _resolve_okx_config_file(_env_value("OKX_CONFIG_FILE") or cfg.okx.config_file)
    okx_profile_payload = _load_okx_profile(okx_config_file, okx_profile)

    if okx_profile:
        cfg.okx.profile = okx_profile
    if okx_config_file:
        cfg.okx.config_file = okx_config_file

    if not cfg.okx.api_key:
        cfg.okx.api_key = _profile_value(okx_profile_payload, "api_key")
    if not cfg.okx.secret_key:
        cfg.okx.secret_key = _profile_value(okx_profile_payload, "secret_key")
    if not cfg.okx.passphrase:
        cfg.okx.passphrase = _profile_value(okx_profile_payload, "passphrase")
    if "proxy_url" not in raw_okx:
        cfg.okx.proxy_url = _normalize_runtime_proxy_url(_profile_value(okx_profile_payload, "proxy_url"))
    if "use_demo" not in raw_okx:
        profile_demo = okx_profile_payload.get("demo")
        if profile_demo is not None:
            cfg.okx.use_demo = bool(profile_demo)

    api_key = _env_value("OKX_API_KEY")
    secret_key = _env_value("OKX_SECRET_KEY")
    passphrase = _env_value("OKX_PASSPHRASE")
    use_demo = _env_value("OKX_USE_DEMO")
    proxy_url = _env_value("OKX_PROXY_URL")
    allow_order_placement = _env_value("QUANT_LAB_ALLOW_ORDER_PLACEMENT")
    database_url = _env_value("QUANT_LAB_DATABASE_URL")
    telegram_bot_token = _env_value("TELEGRAM_BOT_TOKEN")
    telegram_chat_id = _env_value("TELEGRAM_CHAT_ID")
    email_enabled = _env_bool("ALERT_EMAIL_ENABLED")
    email_from = _env_value("ALERT_EMAIL_FROM")
    email_to = _env_csv("ALERT_EMAIL_TO")
    email_subject_prefix = _env_value("ALERT_EMAIL_SUBJECT_PREFIX")
    smtp_host = _env_value("SMTP_HOST")
    smtp_port = _env_int("SMTP_PORT")
    smtp_username = _env_value("SMTP_USERNAME")
    smtp_password = _env_value("SMTP_PASSWORD")
    smtp_use_tls = _env_bool("SMTP_USE_TLS")
    smtp_use_ssl = _env_bool("SMTP_USE_SSL")
    market_data_provider = _env_value("MARKET_DATA_PROVIDER")
    market_data_base_url = _env_value("MARKET_DATA_BASE_URL")
    market_data_proxy_url = _env_value("MARKET_DATA_PROXY_URL")
    market_data_timeout_seconds = _env_float("MARKET_DATA_TIMEOUT_SECONDS")
    market_data_api_key = _env_value("MARKET_DATA_API_KEY")
    market_data_extra_headers = _env_json("MARKET_DATA_EXTRA_HEADERS_JSON")
    market_data_provider_options = _env_json("MARKET_DATA_PROVIDER_OPTIONS_JSON")
    research_ai_enabled = _env_bool("RESEARCH_AI_ENABLED")
    research_ai_provider = _env_value("RESEARCH_AI_PROVIDER")
    research_ai_base_url = _env_value("RESEARCH_AI_BASE_URL")
    research_ai_api_key = _env_value("RESEARCH_AI_API_KEY")
    research_ai_model = _env_value("RESEARCH_AI_MODEL")
    research_ai_timeout_seconds = _env_float("RESEARCH_AI_TIMEOUT_SECONDS")
    research_ai_temperature = _env_float("RESEARCH_AI_TEMPERATURE")
    research_ai_max_output_tokens = _env_int("RESEARCH_AI_MAX_OUTPUT_TOKENS")
    research_ai_max_retries = _env_int("RESEARCH_AI_MAX_RETRIES")
    research_ai_system_prompt = _env_value("RESEARCH_AI_SYSTEM_PROMPT")
    research_ai_provider_options = _env_json("RESEARCH_AI_PROVIDER_OPTIONS_JSON")
    research_agent_enabled = _env_bool("RESEARCH_AGENT_ENABLED")
    research_agent_provider = _env_value("RESEARCH_AGENT_PROVIDER")
    research_agent_base_url = _env_value("RESEARCH_AGENT_BASE_URL")
    research_agent_api_key = _env_value("RESEARCH_AGENT_API_KEY")
    research_agent_timeout_seconds = _env_float("RESEARCH_AGENT_TIMEOUT_SECONDS")
    research_agent_max_retries = _env_int("RESEARCH_AGENT_MAX_RETRIES")
    research_agent_workflow_path = _env_value("RESEARCH_AGENT_WORKFLOW_PATH")
    research_agent_probe_path = _env_value("RESEARCH_AGENT_PROBE_PATH")
    research_agent_local_repo_path = _env_value("RESEARCH_AGENT_LOCAL_REPO_PATH")
    research_agent_python_executable = _env_value("RESEARCH_AGENT_PYTHON_EXECUTABLE")
    research_agent_provider_options = _env_json("RESEARCH_AGENT_PROVIDER_OPTIONS_JSON")

    if api_key:
        cfg.okx.api_key = api_key
    if secret_key:
        cfg.okx.secret_key = secret_key
    if passphrase:
        cfg.okx.passphrase = passphrase
    if use_demo is not None:
        cfg.okx.use_demo = use_demo.lower() == "true"
    if proxy_url:
        cfg.okx.proxy_url = _normalize_runtime_proxy_url(proxy_url)
    if allow_order_placement is not None:
        cfg.trading.allow_order_placement = allow_order_placement.lower() == "true"
    if database_url:
        cfg.database.url = database_url
    if telegram_bot_token:
        cfg.alerts.telegram_bot_token = telegram_bot_token
    if telegram_chat_id:
        cfg.alerts.telegram_chat_id = telegram_chat_id
    if email_enabled is not None:
        cfg.alerts.email_enabled = email_enabled
    if email_from:
        cfg.alerts.email_from = email_from
    if email_to is not None:
        cfg.alerts.email_to = email_to
    if email_subject_prefix:
        cfg.alerts.email_subject_prefix = email_subject_prefix
    if smtp_host:
        cfg.alerts.smtp_host = smtp_host
    if smtp_port is not None:
        cfg.alerts.smtp_port = smtp_port
    if smtp_username:
        cfg.alerts.smtp_username = smtp_username
    if smtp_password:
        cfg.alerts.smtp_password = smtp_password
    if smtp_use_tls is not None:
        cfg.alerts.smtp_use_tls = smtp_use_tls
    if smtp_use_ssl is not None:
        cfg.alerts.smtp_use_ssl = smtp_use_ssl
    if market_data_provider:
        cfg.market_data.provider = market_data_provider
    if market_data_base_url:
        cfg.market_data.base_url = market_data_base_url
    if market_data_proxy_url:
        cfg.market_data.proxy_url = _normalize_runtime_proxy_url(market_data_proxy_url)
    if market_data_timeout_seconds is not None:
        cfg.market_data.timeout_seconds = market_data_timeout_seconds
    if market_data_api_key:
        cfg.market_data.api_key = market_data_api_key
    if isinstance(market_data_extra_headers, dict):
        cfg.market_data.extra_headers = {
            str(key): str(value) for key, value in market_data_extra_headers.items()
        }
    if isinstance(market_data_provider_options, dict):
        cfg.market_data.provider_options = market_data_provider_options
    if research_ai_enabled is not None:
        cfg.research_ai.enabled = research_ai_enabled
    if research_ai_provider:
        cfg.research_ai.provider = research_ai_provider
    if research_ai_base_url:
        cfg.research_ai.base_url = research_ai_base_url
    if research_ai_api_key:
        cfg.research_ai.api_key = research_ai_api_key
    if research_ai_model:
        cfg.research_ai.model = research_ai_model
    if research_ai_timeout_seconds is not None:
        cfg.research_ai.timeout_seconds = research_ai_timeout_seconds
    if research_ai_temperature is not None:
        cfg.research_ai.temperature = research_ai_temperature
    if research_ai_max_output_tokens is not None:
        cfg.research_ai.max_output_tokens = research_ai_max_output_tokens
    if research_ai_max_retries is not None:
        cfg.research_ai.max_retries = research_ai_max_retries
    if research_ai_system_prompt:
        cfg.research_ai.default_system_prompt = research_ai_system_prompt
    if isinstance(research_ai_provider_options, dict):
        cfg.research_ai.provider_options = research_ai_provider_options
    if research_agent_enabled is not None:
        cfg.research_agent.enabled = research_agent_enabled
    if research_agent_provider:
        cfg.research_agent.provider = research_agent_provider
    if research_agent_base_url:
        cfg.research_agent.base_url = research_agent_base_url
    if research_agent_api_key:
        cfg.research_agent.api_key = research_agent_api_key
    if research_agent_timeout_seconds is not None:
        cfg.research_agent.timeout_seconds = research_agent_timeout_seconds
    if research_agent_max_retries is not None:
        cfg.research_agent.max_retries = research_agent_max_retries
    if research_agent_workflow_path:
        cfg.research_agent.workflow_path = research_agent_workflow_path
    if research_agent_probe_path:
        cfg.research_agent.probe_path = research_agent_probe_path
    if cfg.research_agent.local_repo_path:
        cfg.research_agent.local_repo_path = _resolve_runtime_path(base_dir, cfg.research_agent.local_repo_path)
    if research_agent_local_repo_path:
        cfg.research_agent.local_repo_path = _resolve_runtime_path(base_dir, research_agent_local_repo_path)
    if cfg.research_agent.python_executable:
        cfg.research_agent.python_executable = _resolve_runtime_command_path(base_dir, cfg.research_agent.python_executable)
    if research_agent_python_executable:
        cfg.research_agent.python_executable = _resolve_runtime_command_path(base_dir, research_agent_python_executable)
    if isinstance(research_agent_provider_options, dict):
        cfg.research_agent.provider_options = research_agent_provider_options

    return AppConfig.model_validate(cfg.model_dump())


def ensure_storage_dirs(storage: StorageConfig) -> None:
    storage.data_dir.mkdir(parents=True, exist_ok=True)
    storage.raw_dir.mkdir(parents=True, exist_ok=True)
    storage.report_dir.mkdir(parents=True, exist_ok=True)


def update_instrument_section(config_path: Path, instrument_data: dict[str, Any]) -> None:
    update_config_section(config_path, "instrument", instrument_data)


def update_trading_section(config_path: Path, trading_data: dict[str, Any]) -> None:
    update_config_section(config_path, "trading", trading_data)


def update_config_section(config_path: Path, section_name: str, section_data: dict[str, Any]) -> None:
    raw = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    section = raw.setdefault(section_name, {})
    section.update(section_data)
    config_path.write_text(
        yaml.safe_dump(raw, sort_keys=False, allow_unicode=True),
        encoding="utf-8",
    )


def _resolve_path(base_dir: Path, path: Path) -> Path:
    return path if path.is_absolute() else (base_dir / path).resolve()


def _resolve_runtime_path(base_dir: Path, raw_path: str | Path) -> Path:
    raw_value = str(raw_path).strip()
    if _looks_like_windows_absolute_path(raw_value) and _is_wsl_runtime():
        candidate = _windows_path_to_wsl(raw_value)
    else:
        candidate = Path(raw_value).expanduser()
    return candidate if candidate.is_absolute() else (base_dir / candidate).resolve()


def _resolve_runtime_command_path(base_dir: Path, raw_value: str) -> str:
    candidate = str(raw_value or "").strip()
    if not candidate:
        return candidate
    if (
        candidate.startswith(".")
        or "/" in candidate
        or "\\" in candidate
        or _looks_like_windows_absolute_path(candidate)
        or Path(candidate).is_absolute()
    ):
        return str(_resolve_runtime_path(base_dir, candidate))
    return candidate


def _resolve_database_url(base_dir: Path, url: str) -> str:
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        return url

    raw_path = url[len(prefix) :]
    if raw_path.startswith("/"):
        return url

    resolved = (base_dir / raw_path).resolve()
    return f"{prefix}{resolved.as_posix()}"


def _normalized_token(value: Any) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        raise ValueError("value must be a non-empty string")
    return normalized


def _require_positive(name: str, value: float) -> None:
    if float(value) <= 0:
        raise ValueError(f"{name} must be > 0")


def _require_non_negative(name: str, value: float) -> None:
    if float(value) < 0:
        raise ValueError(f"{name} must be >= 0")


def _require_positive_int(name: str, value: int) -> None:
    if int(value) <= 0:
        raise ValueError(f"{name} must be a positive integer")


def _require_non_negative_int(name: str, value: int) -> None:
    if int(value) < 0:
        raise ValueError(f"{name} must be >= 0")


def _require_fraction(
    name: str,
    value: float,
    *,
    allow_zero: bool,
    upper_bound: float = 1.0,
) -> None:
    numeric_value = float(value)
    if allow_zero:
        if not (0.0 <= numeric_value <= upper_bound):
            raise ValueError(f"{name} must be between 0 and {upper_bound}")
        return
    if not (0.0 < numeric_value <= upper_bound):
        raise ValueError(f"{name} must be in the interval (0, {upper_bound}]")


def _require_bar(name: str, value: str) -> Any:
    try:
        return bar_to_timedelta(str(value))
    except Exception as exc:
        raise ValueError(f"{name} is invalid: {value}") from exc


def _env_value(name: str) -> str | None:
    value = os.getenv(name)
    return value if value not in {"", None} else None


def _env_bool(name: str) -> bool | None:
    value = _env_value(name)
    if value is None:
        return None
    return value.strip().lower() in {"1", "true", "yes", "y", "on"}


def _env_int(name: str) -> int | None:
    value = _env_value(name)
    if value is None:
        return None
    return int(value)


def _env_float(name: str) -> float | None:
    value = _env_value(name)
    if value is None:
        return None
    return float(value)


def _env_csv(name: str) -> list[str] | None:
    value = _env_value(name)
    if value is None:
        return None
    items = [item.strip() for item in value.split(",") if item.strip()]
    return items


def _env_json(name: str) -> dict[str, Any] | None:
    value = _env_value(name)
    if value is None:
        return None
    parsed = yaml.safe_load(value)
    if not isinstance(parsed, dict):
        raise ValueError(f"{name} must decode into a JSON/YAML object")
    return parsed


def _resolve_okx_config_file(raw_path: str | Path | None) -> Path | None:
    if raw_path:
        candidate = Path(raw_path).expanduser()
        if candidate.exists():
            return candidate.resolve()
        return candidate

    default_path = Path.home() / ".okx" / "config.toml"
    if default_path.exists():
        return default_path.resolve()

    windows_home = _env_value("USERPROFILE")
    if windows_home:
        converted = _windows_path_to_wsl(windows_home)
        candidate = converted / ".okx" / "config.toml"
        if candidate.exists():
            return candidate.resolve()

    users_root = Path("/mnt/c/Users")
    if users_root.exists():
        preferred = _env_value("USERNAME")
        if preferred:
            candidate = users_root / preferred / ".okx" / "config.toml"
            if candidate.exists():
                return candidate.resolve()
        for candidate in sorted(users_root.glob("*/.okx/config.toml")):
            if candidate.exists():
                return candidate.resolve()

    return None


def _load_okx_profile(config_file: Path | None, profile_name: str | None) -> dict[str, Any]:
    if config_file is None or not config_file.exists():
        return {}

    with config_file.open("rb") as handle:
        payload = tomllib.load(handle)

    profiles = payload.get("profiles")
    if not isinstance(profiles, dict):
        return {}

    resolved_profile = profile_name or payload.get("default_profile")
    if not isinstance(resolved_profile, str) or not resolved_profile:
        return {}

    profile = profiles.get(resolved_profile)
    return profile if isinstance(profile, dict) else {}


def _profile_value(profile: dict[str, Any], key: str) -> str | None:
    value = profile.get(key)
    if value in {"", None}:
        return None
    return str(value)


def _normalize_runtime_proxy_url(proxy_url: str | None) -> str | None:
    if not proxy_url:
        return None

    parsed = urlsplit(proxy_url)
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        return proxy_url
    if not _is_wsl_runtime():
        return proxy_url

    gateway = _wsl_default_gateway()
    if not gateway:
        return proxy_url

    auth = ""
    if parsed.username:
        auth = parsed.username
        if parsed.password:
            auth = f"{auth}:{parsed.password}"
        auth = f"{auth}@"

    port = f":{parsed.port}" if parsed.port else ""
    netloc = f"{auth}{gateway}{port}"
    return urlunsplit((parsed.scheme, netloc, parsed.path, parsed.query, parsed.fragment))


def _is_wsl_runtime() -> bool:
    if _env_value("WSL_DISTRO_NAME"):
        return True

    proc_version = Path("/proc/version")
    if not proc_version.exists():
        return False

    try:
        return "microsoft" in proc_version.read_text(encoding="utf-8").lower()
    except OSError:
        return False


def _wsl_default_gateway() -> str | None:
    try:
        result = subprocess.run(
            ["sh", "-lc", "ip route show default | awk '/default/ {print $3; exit}'"],
            check=True,
            capture_output=True,
            text=True,
            timeout=3,
        )
    except (OSError, subprocess.SubprocessError):
        return None

    value = result.stdout.strip()
    return value or None


def _windows_path_to_wsl(raw_path: str) -> Path:
    normalized = raw_path.replace("\\", "/")
    if len(normalized) >= 3 and normalized[1:3] == ":/":
        drive = normalized[0].lower()
        suffix = normalized[3:]
        return Path("/mnt") / drive / suffix
    return Path(normalized)


def _looks_like_windows_absolute_path(raw_path: str) -> bool:
    normalized = str(raw_path or "").strip().replace("\\", "/")
    return len(normalized) >= 3 and normalized[1:3] == ":/"
