# High Weight Long Strategy

## Scope

- Config: `config/high_weight_btc.yaml`
- Symbol: `BTC-USDT-SWAP`
- Variant: `high_weight_long`
- Signal bar: `4H`
- Execution bar: `1m`

## Factors In Use

Only high-weight factors with stable local history are included in this version.

| Factor | Weight | Data source | Current mapping |
| --- | ---: | --- | --- |
| Daily 200 EMA position | 6 | 4H OHLCV resampled to completed daily bars | Price above completed daily 200 EMA and EMA slope positive scores highest |
| 4H slow EMA slope | 5 | 4H OHLCV | Positive 4H slow EMA slope gets higher score |
| Multi-timeframe alignment | 6 | 4H OHLCV + completed daily bars | `close > trend_ema`, `ema_fast > ema_slow`, daily close above daily 200 EMA |
| Donchian breakout strength | 5 | 4H OHLCV | Breakout distance relative to ATR, penalizing no-breakout and overextended chase |
| Breakout volume ratio | 6 | 4H OHLCV | Current quote volume vs rolling average |
| Basis regime | 5 | OKX mark/index 4H history | Mild positive basis scores highest, extreme premium or discount scores low |

## Factors Deferred

- OI-price confirmation
- CVD / taker delta
- Other missing high-weight external factors

These are intentionally ignored for now instead of replaced by weak proxies.

## Score Logic

- Score is the weighted average of available high-weight factors, normalized to `0-100`.
- Core gate score uses:
  - Daily 200 EMA position
  - Multi-timeframe alignment
  - Donchian breakout strength
- Core gate must be `>= 12`.

## Entry Logic

Open long only when all conditions hold:

- `close > trend_ema`
- `ema_fast > ema_slow`
- `high_weight_core_score >= 12`
- `strategy_score >= 55`

Risk tier by score:

- `>= 70`: full configured risk
- `55-69.99`: half configured risk
- `< 55`: no entry

## Hold / Exit

- After entry, the strategy does not require every bar to fully re-qualify.
- Position is held while the 4H trend structure remains intact.
- Exit when any of these happens:
  - `close <= trend_ema`
  - `ema_fast <= ema_slow`
  - ATR trailing stop is hit intrabar

## Notes

- Funding is still used in execution realism and cost modeling.
- Long history for funding from OKX is incomplete, so older missing timestamps still use the existing conservative fallback model.
- This version is intentionally BTC-first. It is not yet promoted as the default ETH strategy.
