# Quant-Lab 接手导览（2026-03-29）

这份文档面向准备接手 `quant-lab` 的维护者。

目标不是重复 README 的命令清单，而是把当前仓库已经落地的运行形态、关键入口、状态存储、执行闸门和已验证事实讲清楚，让后续维护时先建立正确心智模型。

开始任何代码修改前，先读：

- [docs/change_preflight_checklist_2026-03-31.md](docs/change_preflight_checklist_2026-03-31.md)
- [docs/iteration_readiness_checklist_2026-03-31.md](docs/iteration_readiness_checklist_2026-03-31.md)
- [docs/modification_plan_2026-03-31.md](docs/modification_plan_2026-03-31.md)

如果讨论已经进入产品化、值守落地或 live rollout，继续读：

- [docs/productization_gap_assessment_2026-03-31.md](docs/productization_gap_assessment_2026-03-31.md)

## 1. 当前默认形态

以 `config/settings.yaml` 为准，当前仓库默认不是“单 BTC 回测脚手架”，而是一个已经扩到研究、demo 执行和服务监控的一体化本地系统：

- 组合默认标的：`BTC-USDT-SWAP` + `ETH-USDT-SWAP`
- 当前默认策略：`breakout_retest_4h`
- 当前默认变体：`breakout_retest_regime`
- 信号周期：`4H`
- 执行周期：`1m`
- OKX 模式：`use_demo: true`
- 交易提交默认值：`trading.allow_order_placement: false`
- 执行审批门：`trading.require_approved_candidate: true`
- 路由器默认值：`trading.strategy_router_enabled: false`
- 本地服务：`FastAPI`
- 运行数据库：`SQLite`
- 报表介质：`HTML + CSV + JSON`

维护时要先接受一个事实：这个仓库现在的“主运行面”是研究 + demo 观察，不是直接实盘。

## 2. 你先看哪几个文件

接手时优先读这几个入口：

- `src/quant_lab/cli.py`
  - 所有 CLI 命令的真实入口，负责把配置、数据、回测、研究、执行和服务串起来
- `src/quant_lab/config.py`
  - 运行配置的唯一装配层，决定 YAML、`.env`、OKX profile 的覆盖顺序
- `src/quant_lab/strategies/ema_trend.py`
  - 当前策略逻辑主要集中在这里，不是“一策略一文件”的组织方式
- `src/quant_lab/backtest/engine.py`
  - 回测执行内核，处理进出场、止损、funding、滑点和成交现实性
- `src/quant_lab/execution/planner.py`
  - demo 执行计划层，把信号和账户状态转成订单计划
- `src/quant_lab/service/monitor.py`
  - 服务 API、监控心跳、preflight 检查、仪表盘相关接口都在这里
- `src/quant_lab/service/database.py`
  - SQLite / PostgreSQL 的表模型定义

如果你只能先读 3 个文件，就读 `cli.py`、`config.py`、`monitor.py`。

## 3. 配置是怎么生效的

配置覆盖顺序由 `src/quant_lab/config.py` 决定：

1. YAML 基础配置
2. `.env` 环境变量覆盖
3. 可选 OKX profile / `config.toml` 注入私有凭证、代理、demo 标志

这意味着：

- `config/settings.yaml` 决定策略、组合、风控和服务的默认行为
- `.env` 更适合放密钥、告警通道、数据库 URL、Research AI 配置
- OKX profile 适合放私有 API 密钥和代理参数，不应该硬编码到 YAML

### 哪些参数主要影响回测

- `strategy.*`
- `execution.fee_bps / slippage_bps / latency_minutes / market_impact_*`
- `risk.*`
- `portfolio.symbols`

### 哪些参数会进入 demo 执行

- `okx.*`
- `trading.*`
- `execution.max_leverage`
- `alerts.*`
- `research_ai.*`

不要把“回测假设”和“提交闸门”混在一起看。`fee_bps` 改了影响收益假设，`allow_order_placement` 改了会直接改变提交权限。

## 4. CLI 命令族怎么分层

以 `src/quant_lab/cli.py` 为准，当前命令可以按 5 组理解。

### 4.1 数据与研究准备

- `download`
- `download-public-factors`
- `sync-instrument`
- `sweep`
- `research-trend`

这组命令负责把 `OKX` 公共数据、public factors、参数扫描和研究素材准备出来。

### 4.2 回测与报表

- `backtest`
- `report`

这组是传统量化脚手架核心。

当 `portfolio.symbols` 有多个标的时：

- 先分别生成每个 symbol 的 sleeve 结果
- 再聚合成 portfolio 级 equity / trades / summary / dashboard

### 4.3 研究治理

- `research-create-task`
- `research-list-tasks`
- `research-register-candidate`
- `research-list-candidates`
- `research-evaluate-candidate`
- `research-approve-candidate`
- `research-backtest-candidate`
- `research-bind-candidate`
- `research-set-route`
- `research-overview`
- `research-ai-status`
- `research-ai-run`
- `research-materialize-top`
- `research-promote-top`
- `research-routed-backtest`

这是仓库区别于普通回测项目的关键层。

主链路是：

`research task -> strategy candidate -> evaluation report -> approval decision -> execution binding/router`

也就是说，这个系统已经把“策略审批后才能进入 demo 执行”的治理过程做成了 CLI/API/DB 一体流程。

### 4.4 Demo 执行

- `demo-account`
- `demo-plan`
- `demo-reconcile`
- `demo-align-leverage`
- `demo-preflight`
- `demo-execute`
- `demo-loop`
- `demo-drill`
- `demo-portfolio-plan`
- `demo-portfolio-reconcile`
- `demo-portfolio-loop`
- `demo-portfolio-drill`

其中：

- `demo-preflight` 是最重要的安全入口
- `demo-plan` / `demo-portfolio-plan` 只生成计划，不提交
- `demo-execute` / `demo-loop` 只有通过多重闸门后才允许提交
- `demo-portfolio-*` 说明组合执行底座已经不是 TODO，而是已接入当前主线

### 4.5 服务与告警

- `service-init-db`
- `service-step`
- `service-api`
- `alert-test`

这里不是独立前后端项目，而是 Python 直接提供本地 API 和 HTML 仪表盘。

## 5. 四条端到端链路

### 5.1 数据采集链路

主线：

`OKX public API -> parquet/json cache -> signal/factor 输入`

主要落点：

- `data/raw/*_4H.parquet`
- `data/raw/*_1m.parquet`
- `data/raw/*_funding.parquet`
- `data/raw/*_mark_price_4H.parquet`
- `data/raw/*_index_4H.parquet`
- `data/raw/*_instrument.json`

这一层的目标是尽量把回测和研究建立在本地缓存上，而不是每次都依赖实时网络。

### 5.2 策略与回测链路

主线：

`OKX public data -> signal frame -> backtest engine -> metrics -> report artifacts`

关键点：

- 信号逻辑主要在 `src/quant_lab/strategies/ema_trend.py`
- 同一文件已经承载 `ema_cross`、`breakout_retest_regime`、`high_weight_long` 等变体
- 回测引擎在 `src/quant_lab/backtest/engine.py`，不是简单的净值曲线拼接，而是包含：
  - 止损
  - funding
  - 滑点
  - 延迟
  - 流动性约束
  - market impact

组合模式不是多标的一次性大回测，而是：

1. 每个 symbol 先独立生成 sleeve
2. 再由 `src/quant_lab/backtest/portfolio.py` 聚合

### 5.3 研究治理链路

主线：

`task -> candidate -> evaluation -> approval -> bind/route -> demo gate`

状态主要落库到：

- `research_tasks`
- `strategy_candidates`
- `evaluation_reports`
- `approval_decisions`

维护时要特别注意：

- 研究治理不是附属文档，而是代码路径上的真闸门
- 当 `trading.require_approved_candidate = true` 时，demo submit 不会因为“你有信号”就放行
- 绑定 candidate 和启用 router 是两件不同的事

### 5.4 Demo 执行与服务监控链路

主线：

`demo-preflight / demo-plan -> order plan -> duplicate-submit state -> heartbeat / alerts -> FastAPI dashboard/client`

关键对象：

- 计划层：`src/quant_lab/execution/planner.py`
- 路由层：`src/quant_lab/execution/strategy_router.py`
- 风控层：`src/quant_lab/risk/portfolio.py`
- 服务层：`src/quant_lab/service/monitor.py`
- client 聚合层：`src/quant_lab/service/client_ops.py`

这一层已经不仅能“给出计划”，还会记录：

- duplicate submit state
- demo loop 心跳
- alert 投递结果
- 最新 runtime snapshot

但 docs 也已经明确指出：组合执行底座已具备，UI 视角仍偏单标的。

## 6. 状态存储在哪里

### 6.1 文件态

- `data/raw`
  - 原始行情、funding、mark/index、盘口与 instrument metadata
- `data/reports`
  - 回测、组合、sweep、trend research 的 HTML / CSV / JSON 产物
- `data/demo_executor_state.json`
  - demo 执行去重状态、最近提交计划、最近错误、最近信号
- `data/demo-loop.log`
  - demo loop 运行日志
- `data/service-api.log`
  - service API 运行日志

### 6.2 数据库态

表模型定义在 `src/quant_lab/service/database.py`：

- `runtime_snapshots`
- `service_heartbeats`
- `alert_events`
- `project_task_runs`
- `research_tasks`
- `strategy_candidates`
- `evaluation_reports`
- `approval_decisions`

理解方式很简单：

- 文件态保存大体量市场数据和报表产物
- 数据库态保存系统运行状态、研究审批状态和服务事件

## 7. 真正影响提交下单的闸门

如果要回答“为什么当前不下单”，优先看 `demo-preflight`。

当前提交相关的核心闸门有：

- `okx.use_demo`
- `trading.allow_order_placement`
- `trading.require_approved_candidate`
- `trading.strategy_router_enabled`
- CLI 传参 `--submit`
- CLI 传参 `--confirm OKX_DEMO`

还要补一层隐含约束：

- 账户 `posMode` 是否与配置匹配
- 是否存在 executable instructions
- candidate 是否已审批且 scope 兼容 `demo`
- router 开启时，route key 是否能解析到 approved candidate

这意味着：

- “有信号”不等于“会提交”
- “能生成 plan”不等于“submit ready”
- “router 关闭”时仍可能被审批门挡住

## 8. 2026-03-29 已验证事实

下面这些不是猜测，而是在当前仓库和当前环境里实际验证过的事实。

### 8.1 测试与命令

- 在 `WSL Ubuntu-24.04` 中执行 `.venv/bin/python -m pytest -q`，结果为 `108 passed`
- CLI 帮助在 WSL 中可正常列出全部命令族
- 当前 `.venv` 为 Linux/WSL 目录布局，根目录下是 `.venv/bin`，不是 Windows PowerShell 常见的 `.venv/Scripts`

### 8.2 当前 `demo-preflight` 真实状态

以 2026-03-29 的实测结果为准：

- `demo_trading.mode = submit_blocked`
- `demo_trading.ready = false`
- 阻塞原因有两条：
  - `trading.allow_order_placement=false`
  - `no approved execution candidate is bound to the current strategy`

这说明系统现在是“可观测、可计划、不可直接提交”的安全状态。

### 8.3 当前服务接口可达

实测可返回：

- `GET /health`
- `GET /runtime/preflight`
- `GET /research/overview`

其中：

- `/health` 返回当前 symbol 和 strategy
- `/runtime/preflight` 能把 demo readiness、OKX connectivity、execution approval、strategy router 状态一次性汇总出来
- `/research/overview` 当前返回空任务和空 candidate 集合，说明研究治理接口在，但当前库里没有已登记的研究条目

### 8.4 当前数据与产物是“真运行痕迹”

当前仓库里已经存在：

- `BTC` / `ETH` 的 `1m`、`4H`、funding、mark/index、books 等原始文件
- portfolio 级报表，例如 `portfolio_btc_eth_breakout_retest_4h_dashboard.html`
- sleeve 级报表，例如 `BTC-USDT-SWAP_breakout_retest_4h_sleeve_dashboard.html`
- trend research、sweep、high-weight 策略相关产物
- 本地 `SQLite` 数据库 `data/quant_lab.db`

所以接手时不要把仓库当成“刚搭好、还没跑过”的模板。

## 9. 哪些能力已经可用，哪些还只是半成品

### 已可用

- 公共数据下载与本地缓存
- 单标 / 双标回测
- 组合报表
- demo plan / reconcile / drill / loop
- preflight 检查
- 服务 API 与本地 HTML 面板
- 研究治理与 candidate 审批链路
- strategy router 的基础设施与 routed backtest

### 仍有明显边界

- 组合执行 UI 仍偏单标的视角
- 更严格的组合级动态风控闸门仍可继续加强
- 更完整的生产级监控仍偏轻量，后续更适合接 Grafana
- funding 历史完整性仍有限，旧区间仍会落到保守 fallback
- 某些执行保护逻辑还停留在 plan / scaffold 层，而不是完整交易所托管保护单编排

## 10. 新维护者的建议接手顺序

建议按下面顺序接手：

1. 先读 `config/settings.yaml`，确认当前默认运行形态
2. 读 `cli.py`，把命令族和实际边界对应起来
3. 跑一遍 `demo-preflight`
4. 打开 `data/reports` 中当前组合报表看产物形态
5. 读 `monitor.py` 和 `database.py`，确认服务态和状态表
6. 再进入策略、回测、router 和风控细节

如果下一步要做开发，优先分清三类改动：

- 研究层改动：影响 signal / candidate / evaluation
- 执行层改动：影响 plan / submit / reconcile / leverage
- 服务层改动：影响 dashboard / client / project task / alerts

先分层，再改代码，否则很容易把研究逻辑、执行闸门和服务可视化缠在一起。
