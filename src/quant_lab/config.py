from __future__ import annotations

import os
import subprocess
import tomllib
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlunsplit

import yaml
from dotenv import load_dotenv
from pydantic import BaseModel, Field


class OkxConfig(BaseModel):
    rest_base_url: str = "https://www.okx.com"
    profile: str | None = None
    config_file: Path | None = None
    api_key: str | None = None
    secret_key: str | None = None
    passphrase: str | None = None
    proxy_url: str | None = None
    use_demo: bool = False


class InstrumentConfig(BaseModel):
    symbol: str = "BTC-USDT-SWAP"
    instrument_type: str = "SWAP"
    contract_value: float = 0.01
    contract_value_currency: str = "BTC"
    lot_size: float = 1.0
    min_size: float = 1.0
    tick_size: float | None = None
    settle_currency: str | None = None


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


class RiskConfig(BaseModel):
    risk_per_trade: float = 0.02
    weekly_drawdown_pause: float = 0.06
    portfolio_max_total_risk: float = 0.03
    portfolio_max_same_direction_risk: float = 0.025


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


class PortfolioConfig(BaseModel):
    symbols: list[str] = Field(default_factory=list)


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


class AppConfig(BaseModel):
    okx: OkxConfig = Field(default_factory=OkxConfig)
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

    return cfg


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


def _resolve_database_url(base_dir: Path, url: str) -> str:
    prefix = "sqlite:///"
    if not url.startswith(prefix):
        return url

    raw_path = url[len(prefix) :]
    if raw_path.startswith("/"):
        return url

    resolved = (base_dir / raw_path).resolve()
    return f"{prefix}{resolved.as_posix()}"


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
