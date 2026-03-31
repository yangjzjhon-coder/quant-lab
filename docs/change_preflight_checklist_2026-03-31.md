# Quant-Lab 修改前检查清单（2026-03-31）

这份文档是 `quant-lab` 的修改前必读清单。

目标不是增加流程负担，而是避免在一个已经包含回测、风控、研究治理、demo 执行、服务监控和本地状态库的仓库里，出现“直接改代码、事后才发现口径漂移或回归”的情况。

建议：每次开始任何代码修改前，先从头看一遍本文，再动手。

如果本次不是单点修改，而是一轮联动迭代，继续看：

- [docs/iteration_readiness_checklist_2026-03-31.md](docs/iteration_readiness_checklist_2026-03-31.md)

如果本次目标已经涉及产品化、值守落地或 live rollout，继续看：

- [docs/productization_gap_assessment_2026-03-31.md](docs/productization_gap_assessment_2026-03-31.md)

## 1. 先确认这次修改站在哪个基线之上

开始改代码前先记录当前基线，不要直接在模糊状态下修改：

- 当前分支是什么
- `git status --short` 是否干净
- 本次修改是基于哪个配置文件
- 当前数据库和报告产物是否已经带有历史状态

最低要求：

- 先看一眼 `git status --short`
- 明确当前工作区里哪些改动是旧改动，哪些是这次要做的改动
- 不要把“已有脏改动”和“本轮迭代改动”混成一团

如果工作区不干净，先在心里回答清楚一个问题：后面发现问题时，你能不能分辨这是老问题还是新问题。

## 2. 先确认当前仓库是不是绿基线

开始新迭代前，不要默认“仓库本来就是好的”。

先跑当前统一测试入口，确认现在是绿基线还是红基线：

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

如果不是全绿：

- 先记录失败点
- 先判断是既有问题还是当前环境问题
- 先决定这轮是否要把基线修绿

截至 `2026-03-31`，当前工作区实测观察到：

- 建议测试入口：`.\.venv\Scripts\python.exe -m pytest -q`
- 全局 `pytest` 不在 PATH 中，不应假设 `pytest` 命令可直接用
- 当前工作区存在至少一个已观测到的失败：
  - `tests/test_routed_backtest.py::test_research_routed_backtest_cli_writes_report_and_routing_artifacts`
  - 失败现象：`KeyError('open')`

这条事实是“当前工作区观察结果”，不是永久结论。后续如果基线变化，应同步更新本文。

## 3. 先看运行硬约束，不要绕过主干

在这个仓库里，很多问题不是“代码能不能跑”，而是“是否破坏共享运行时主干”。

修改前至少复读一次：

- [docs/runtime_hard_constraints_2026-03-30.md](docs/runtime_hard_constraints_2026-03-30.md)
- [docs/research_governance_workflow_2026-03-28.md](docs/research_governance_workflow_2026-03-28.md)
- [docs/maintainer_onboarding_2026-03-29.md](docs/maintainer_onboarding_2026-03-29.md)

这几条要视为硬约束：

- `ready / blocked / halt / duplicate / reconcile` 必须来自共享决策源
- 新的 live 或 demo 能力应先落在共享 runtime/helper，再接 CLI 或 service
- `single / portfolio`、`demo / live` 必须是参数化模式，不允许长出平行实现
- 审批、路由、preflight、reconcile 不能被快捷路径绕过

如果一个改动会让 CLI、Service、Dashboard 各自重新判断状态，那通常就是错方向。

## 4. 先审查高风险模块，再决定从哪里下手

不要一上来就改最顺手的地方，要先看改动是否碰到高风险链路。

优先审查这些入口：

- `src/quant_lab/cli.py`
- `src/quant_lab/config.py`
- `src/quant_lab/backtest/engine.py`
- `src/quant_lab/execution/planner.py`
- `src/quant_lab/execution/strategy_router.py`
- `src/quant_lab/service/monitor.py`
- `src/quant_lab/service/demo_runtime.py`

至少回答下面几个问题：

- 这次改动会不会让回测口径和执行口径分叉
- 这次改动会不会复制已有状态判断逻辑
- 这次改动会不会影响 `approved candidate -> route -> submit gate`
- 这次改动会不会让单标和组合模式行为不一致
- 这次改动会不会让服务端和 CLI 输出不一致

如果答案里有一个“不确定”，先补审查，不要直接写代码。

## 5. 先确认数据、配置和产物来源

`quant-lab` 不是只靠代码运行的仓库，它高度依赖配置、缓存数据、数据库状态和报告产物。

修改前要先确认：

- 这次改动依赖哪个配置文件
- 用到哪些 `data/raw` 数据
- 会不会影响 `data/reports` 产物命名或结构
- 会不会影响 `data/quant_lab.db` 的状态解释

最低要求：

- 明确使用的配置入口，通常是 `config/settings.yaml`
- 明确是否会改变 artifact 行为、报告路径、状态落库结构
- 如果修改涉及评估、审批、route、runtime snapshot，要同步考虑历史数据兼容性

## 6. 先收口本轮目标，不要同时改四层

这个仓库的主链路比较长，最怕一轮里同时碰太多层。

一轮修改开始前，先把目标收口到一个主题：

- 稳定性修复
- 回测真实性增强
- 研究治理增强
- 组合执行增强
- 服务/可视化一致性修复

除非是明确的联动重构，否则不要在同一轮里同时大改：

- `engine`
- `planner`
- `router`
- `monitor`

如果不得不联动修改，先把联动边界写出来，再改。

## 7. 先写验收标准，再开始实现

每次修改前，至少先写出下面四件事：

- 哪个行为会变化
- 哪个行为必须保持不变
- 哪些测试要新增
- 哪些旧测试必须继续通过

建议每次修改都至少覆盖以下一种验证：

- 单元测试
- CLI 级回归测试
- service API 行为验证
- 关键 artifact 产物验证

如果改动涉及执行、审批、router、runtime policy，不能只靠“本地看起来能跑”验收。

## 8. 建议的修改前固定动作

每次正式修改前，按这个顺序走一遍：

1. 看本文
2. 看 `runtime_hard_constraints`
3. 看 `git status --short`
4. 跑一次当前测试基线
5. 标记本轮修改主题
6. 标记影响模块
7. 写下验收标准
8. 再开始改代码

## 9. 默认结论

如果你在“直接开始写代码”和“先做基线确认/约束复读/高风险审查”之间犹豫，默认选择后者。

在 `quant-lab` 里，真正昂贵的不是多写两分钟代码，而是：

- 写出平行规则栈
- 把旧问题和新问题混在一起
- 在未确认基线时开始扩展功能
- 让回测、执行、服务三条链路各说各话
