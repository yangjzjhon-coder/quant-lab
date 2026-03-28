# Quant Lab

面向 `OKX BTC-USDT-SWAP` 的本地量化研究与运行脚手架，当前重点是：

- `4h` 均线趋势回测
- 更保守的成交建模
- 风控暂停逻辑
- 本地数据库 + 服务监控 + 可视化面板

这套仓库现在已经可以完成你的第一阶段目标：先把回测系统和本地运行底座搭起来。

## 当前能力

- 拉取 `OKX` 公共历史数据：`4H`、`1m`、`funding rate`
- 使用 `4h` 收盘信号、`1m` 执行数据做更真实的回测
- 计入 `taker fee`、固定滑点、执行延迟、资金费率
- 风控包含：
  - 单笔风险 `<= 2%`
  - 周内回撤达到 `6%` 暂停开新仓
- 输出 `summary.json`、`equity_curve.csv`、`trades.csv`
- 生成回测 HTML 报表和参数扫描 HTML 报表
- 将运行快照、服务心跳、告警事件写入 `SQLite` 或 `PostgreSQL`
- 提供本地 `FastAPI` 服务和运行态可视化面板

## 目录

```text
quant-lab/
  config/
    settings.example.yaml
    settings.yaml
  data/
    raw/
    reports/
  src/quant_lab/
    alerts/
    backtest/
    data/
    reporting/
    risk/
    service/
    strategies/
  tests/
```

## 建议环境

推荐在 `WSL2 Ubuntu 24.04` 内运行，避免 Windows 原生环境下的 Python、TA、Docker 兼容问题。

## 快速开始

1. 进入项目目录

```bash
cd quant-lab
```

2. 创建虚拟环境并安装依赖

```bash
python -m venv .venv
source .venv/bin/activate
pip install -U pip
pip install -e ".[dev]"
```

3. 准备配置

```bash
cp .env.example .env
cp config/settings.example.yaml config/settings.yaml
```

4. 同步合约元数据

```bash
quant-lab sync-instrument --config config/settings.yaml
```

5. 下载回测数据

```bash
quant-lab download --config config/settings.yaml --start 2023-01-01 --end 2026-03-01
```

如果你要按当前推荐方向直接做 `BTC + ETH` 双标的研究，可以继续用同一条命令。
因为 `config/settings.yaml` 里现在已经支持：

```yaml
portfolio:
  symbols:
    - BTC-USDT-SWAP
    - ETH-USDT-SWAP
```

也可以显式指定：

```bash
quant-lab download \
  --config config/settings.yaml \
  --start 2023-01-01 \
  --end 2026-03-01 \
  --symbols BTC-USDT-SWAP,ETH-USDT-SWAP
```

6. 运行回测

```bash
quant-lab backtest --config config/settings.yaml
```

当 `portfolio.symbols` 里有多个标的时，`backtest` 会：

- 分别生成每个标的的 sleeve 回测结果
- 按等权初始资金汇总为一个组合权益曲线
- 额外输出组合级 `summary / equity_curve / trades / sleeves`

如果你只想临时跑单标的，可以这样：

```bash
quant-lab backtest --config config/settings.yaml --symbols BTC-USDT-SWAP
```

7. 生成回测 HTML 报表

```bash
quant-lab report --config config/settings.yaml
```

多标的模式下，`report` 会同时生成：

- 每个标的的 sleeve HTML 报表
- 一个组合级 HTML 报表

## 双标模拟盘执行命令

这一步新增了一套专门给 `BTC + ETH` 双标执行底座用的命令，先不替换你已有的单标命令：

```bash
quant-lab demo-portfolio-plan --config config/settings.yaml
quant-lab demo-portfolio-reconcile --config config/settings.yaml
quant-lab demo-portfolio-drill --config config/settings.yaml
quant-lab demo-portfolio-loop --config config/settings.yaml
```

含义分别是：

- `demo-portfolio-plan`
  - 同时拉取 `BTC + ETH` 最新市场数据并生成双标执行计划
- `demo-portfolio-reconcile`
  - 同时检查 `BTC + ETH` 当前仓位、目标仓位、杠杆与保护性止损状态
- `demo-portfolio-drill`
  - 跑一轮双标演练，输出结构化 JSON，不持续循环
- `demo-portfolio-loop`
  - 按轮询周期持续执行双标 demo loop

当前这套双标执行底座默认按：

- 共享一个 OKX demo 账户
- 按等权方式给每个标的分配 planning equity
- 每个标的分别做去重、防重复提交和执行状态记录

注意：

- 这一步完成的是 `CLI / executor / heartbeat` 层的双标底座
- 现有网页 client 还是以单标视角为主，后面再继续扩成双标界面

8. 运行参数扫描

```bash
quant-lab sweep \
  --config config/settings.yaml \
  --fast 10,20,30 \
  --slow 50,80,120 \
  --atr 1.5,2.0,2.5
```

## 当前默认交易假设

- 标的：`BTC-USDT-SWAP`
- 策略：`EMA(20/50)` 趋势切换
- 信号：仅使用 `4h` 收盘确认
- 执行：信号后 `1` 分钟，按 `1m open` 加滑点成交
- 手续费：默认按 `taker`
- 开平仓：按 `market/taker` 保守建模
- 止损：基于 `ATR * multiple`

## 报表产物

默认会在 `data/reports/` 生成：

- `BTC-USDT-SWAP_ema_trend_4h_summary.json`
- `BTC-USDT-SWAP_ema_trend_4h_equity_curve.csv`
- `BTC-USDT-SWAP_ema_trend_4h_trades.csv`
- `BTC-USDT-SWAP_ema_trend_4h_dashboard.html`
- `BTC-USDT-SWAP_ema_trend_4h_sweep.csv`
- `BTC-USDT-SWAP_ema_trend_4h_sweep_dashboard.html`

如果你只想直接看 HTML 报表，可以在文件管理器里打开：

```text
data/reports/BTC-USDT-SWAP_ema_trend_4h_dashboard.html
data/reports/BTC-USDT-SWAP_ema_trend_4h_sweep_dashboard.html
```

## 本地服务与运行面板

初始化数据库：

```bash
quant-lab service-init-db --config config/settings.yaml
```

手动执行一轮监控：

```bash
quant-lab service-step --config config/settings.yaml
```

启动本地服务：

```bash
quant-lab service-api --config config/settings.yaml
```

启动后可访问：

- `http://127.0.0.1:18080/`
  - 运行态可视化面板
- `http://127.0.0.1:18080/client`
  - 本地交易客户端
  - 可直接点击执行 `对账 / 杠杆 dry-run / 杠杆对齐 / 告警测试`
- `http://127.0.0.1:18080/health`
  - 健康检查
- `http://127.0.0.1:18080/runtime/latest`
  - 最新运行快照
- `http://127.0.0.1:18080/heartbeats`
  - 最近服务心跳
- `http://127.0.0.1:18080/alerts`
  - 最近告警
- `http://127.0.0.1:18080/reports/backtest`
  - 回测 HTML 报表
- `http://127.0.0.1:18080/reports/sweep`
  - 参数扫描 HTML 报表

## Windows 一键启动

仓库里已经附带：

- `scripts/QuantLab启动.ps1`
- `scripts/QuantLab停止.ps1`
- `scripts/QuantLab启动.cmd`
- `scripts/QuantLab停止.cmd`
- `scripts/StartQuantLabClient.ps1`
- `scripts/StartQuantLabClient.cmd`

同时我也放了桌面快捷入口：

- `C:\Users\Administrator\Desktop\QuantLab启动.cmd`
- `C:\Users\Administrator\Desktop\QuantLab停止.cmd`

推荐你以后直接双击桌面的 `QuantLab启动.cmd`。

更简单的桌面入口也已经放好：

- `C:\Users\Administrator\Desktop\启动量化系统.cmd`
- `C:\Users\Administrator\Desktop\停止量化系统.cmd`
- `C:\Users\Administrator\Desktop\打开交易客户端.cmd`

这套脚本会：

- 在 `WSL Ubuntu-24.04` 里后台启动 `quant-lab service-api`
- 刷新 `127.0.0.1:18080 -> WSL_IP:18080` 的 `portproxy`
- 检查 `http://127.0.0.1:18080/health`
- 自动打开运行面板

如果你只想直接进入交易客户端，双击：

- `scripts/StartQuantLabClient.cmd`
- `C:\Users\Administrator\Desktop\打开交易客户端.cmd`

它会先确保服务已经起来，然后直接打开：

- `http://127.0.0.1:18080/client`

注意：

- 第一次冷启动可能要 `20-45 秒`
- 如果刚双击后浏览器还没打开，不要重复启动，先等脚本跑完

如果你想在 WSL 里手动控制，也可以用：

```bash
cd /mnt/e/quant-lab
scripts/quant_lab_service.sh start
scripts/quant_lab_service.sh stop
scripts/quant_lab_service.sh status
scripts/quant_lab_service.sh logs
```

## 告警测试

当前先接了 Telegram 骨架，默认关闭。

```bash
quant-lab alert-test --config config/settings.yaml --message "service layer wired"
```

环境变量示例见：

- `.env.example`

## 数据库

默认使用本地 `SQLite`：

```text
sqlite:///data/quant_lab.db
```

也可以通过环境变量切换到 `PostgreSQL`：

```bash
export QUANT_LAB_DATABASE_URL="postgresql+psycopg://quant_lab:quant_lab@127.0.0.1:5432/quant_lab"
```

仓库中还附带了一个本地 `docker-compose.yml`，可以拉起：

- `PostgreSQL`
- `Grafana`

## 已验证结果

当前这份仓库已经完成并验证过：

- 公共历史数据下载
- 全区间回测
- HTML 报表生成
- 参数扫描
- 实时 `demo-plan` 信号与下单计划生成
- 本地数据库初始化
- 监控快照落库
- 服务 API 实际可访问
- 运行面板可加载快照、心跳、告警、报表入口

一组已跑通的正式回测结果：

- 区间：`2023-01-01` 到 `2026-03-01`
- 期末权益：`59348.7`
- 总收益：`493.49%`
- 最大回撤：`19.84%`
- 交易次数：`98`
- Sharpe：`1.83`

参数扫描当前看到的较优组合：

- `EMA 10/50`
- `ATR stop 2.5`
- 总收益：`778.71%`
- 最大回撤：`16.04%`
- Sharpe：`2.38`

## 已知限制

- `funding rate` 历史数据目前并没有完整覆盖 `2023 ~ 2025`，所以那部分区间的资金费率建模仍然是不完整的。
- `OKX demo trading` 已接入到“账户读取 / 信号计划 / 显式确认后提交订单”的骨架层，但当前版本的保护性止损仍是 `plan-only`，还没有作为单独的 `algo stop order` 自动发到 OKX。
- 服务面板是轻量内置页面，适合当前阶段使用；更完整的生产监控后面再接 `Grafana`。

## OKX 模拟盘执行层

这一步已经补上了安全骨架，默认不会提交订单。

先只看账户与计划：

```bash
quant-lab demo-account --config config/settings.yaml
quant-lab demo-plan --config config/settings.yaml
quant-lab demo-loop --config config/settings.yaml --cycles 1
```

如果你准备接入 `OKX demo` 私钥，需要在 `.env` 里填：

```bash
OKX_API_KEY=...
OKX_SECRET_KEY=...
OKX_PASSPHRASE=...
OKX_USE_DEMO=true
QUANT_LAB_ALLOW_ORDER_PLACEMENT=true
```

然后才能显式提交模拟盘订单：

```bash
quant-lab demo-execute \
  --config config/settings.yaml \
  --submit \
  --confirm OKX_DEMO
```

执行层当前行为：

- 复用现有 `EMA 4h` 信号与风险仓位 sizing
- 读取 `OKX account config / balance / positions / max-size`
- 生成 `open / close / flip` 订单计划
- 开仓单默认会附带 `stop loss`
- 默认只输出计划 JSON，不实际下单
- 只有同时满足以下条件才允许提交：
  - `OKX_USE_DEMO=true`
  - `QUANT_LAB_ALLOW_ORDER_PLACEMENT=true`
  - 命令显式传入 `--submit`
  - 命令显式传入 `--confirm OKX_DEMO`

如果你要让它轮询运行：

```bash
quant-lab demo-loop \
  --config config/settings.yaml \
  --submit \
  --confirm OKX_DEMO
```

轮询执行器当前特性：

- 默认轮询间隔来自 `trading.poll_interval_seconds`
- 会把最近一次已提交计划签名记录到 `data/demo_executor_state.json`
- 如果下一轮检测到完全相同的计划，会跳过重复提交

## 安全提醒

- 不要把新的 `OKX API key`、`secret`、`passphrase`、真实 token 提交进仓库。
- 如果密钥曾经公开暴露，应立即作废并重新生成。
- 目前这版代码做回测只依赖 `OKX public market data`，不需要把私钥硬编码到代码里。

## 下一阶段

- 接 `OKX demo trading`
- 接账户与合约元数据校验
- 增加更真实的订单簿滑点和部分成交模拟
- 接 `PostgreSQL + Grafana + Telegram`
- 再进入模拟盘执行与 7x24 守护进程阶段

## 2026-03 Runtime Additions

新增两个更适合实盘前联调的命令：

```bash
quant-lab demo-preflight --config config/settings.yaml --live-plan
quant-lab demo-drill --config config/settings.yaml
quant-lab demo-reconcile --config config/settings.yaml
quant-lab demo-align-leverage --config config/settings.yaml
```

- `demo-preflight`
  - 检查当前是否满足 `OKX demo` 提交条件
  - 展示 `Telegram / Email` 告警通道是否就绪
  - 可选拉一遍最新市场数据并生成当前计划单，确认“配置 + 公共行情 + 计划器”一起正常
- `demo-drill`
  - 做一次端到端演练
  - 默认 `plan-only`，不会发单
  - 会把本次演练结果、心跳和告警记录写进数据库，服务面板也能看到
- `demo-reconcile`
  - 直接读取 `OKX demo` 当前账户状态、仓位、未成交订单、条件单、杠杆设置
  - 检查 `posMode / trade 权限 / leverage / protective stop` 是否与本地策略配置一致
  - 读取 `data/demo_executor_state.json`，帮助排查“本地认为发过单”和“交易所当前状态”是否对得上
- `demo-align-leverage`
  - 默认先做 dry-run，只告诉你“为了和 `config.execution.max_leverage` 对齐，需要发哪些请求”
  - 加 `--apply --confirm OKX_DEMO` 后才会真正修改 `OKX demo` 杠杆
  - 如果 `cross` 模式下还有在场的 `TP/SL / conditional algo order`，命令会提前拒绝并说明原因，因为 `OKX` 会阻止这类杠杆调整
  - 如果你确认要让系统代为执行“撤止损 -> 调杠杆 -> 重新挂止损”，可以额外加 `--rearm-protective-stop`

如果你以后真的要打到 `OKX demo`，仍然要显式确认：

```bash
quant-lab demo-drill \
  --config config/settings.yaml \
  --submit \
  --confirm OKX_DEMO
```

Windows 侧也补了一键脚本：

- `scripts/DemoPreflight.cmd`
- `scripts/DemoDrill.cmd`

启动服务脚本现在也会在启动完成后直接提示：

- 当前 `demo trading mode`
- 哪些告警通道已经 ready
- 如果还不能提交，会直接显示前几条阻塞原因
