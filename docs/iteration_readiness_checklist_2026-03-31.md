# Quant-Lab 迭代前 Readiness 清单（2026-03-31）

这份文档用于“开始一轮迭代前”的准备，不是“开始改一个文件前”的准备。

它和 [docs/change_preflight_checklist_2026-03-31.md](docs/change_preflight_checklist_2026-03-31.md) 的区别是：

- `change_preflight_checklist` 约束单次修改前的动作
- 本文约束一整轮迭代启动前的动作

如果本轮只做一个小修复，先看 `change_preflight_checklist` 即可。
如果本轮要做一组联动改动、一次主题迭代、一次版本推进，先看本文。

如果需要固定某个时间点的正式执行路线，继续看：

- [docs/modification_plan_2026-03-31.md](docs/modification_plan_2026-03-31.md)

## 1. 先定义这轮迭代到底要解决什么

开始前先把本轮目标收口成一句话，不要用“顺便一起改了”作为默认策略。

建议每轮只选一个主题：

- 稳定性修复
- 回测真实性增强
- 研究治理增强
- 组合执行增强
- 服务端与可视化一致性增强
- Stage 0 live rollout 准备

同时写清楚本轮不做什么。

如果“不做什么”写不出来，通常说明本轮边界还没有收口。

## 2. 先确认这轮迭代的起跑线是否可信

开始前先确认当前版本是不是一个可理解的基线：

- 当前工作区改动范围是否已经盘清
- 当前测试基线是否已知
- 当前配置入口是否明确
- 当前数据、报告、数据库状态是否能解释

最低要求：

- 先过一遍 [docs/change_preflight_checklist_2026-03-31.md](docs/change_preflight_checklist_2026-03-31.md)
- 先确认当前红测、脏工作区、未解释产物是否已经被记录
- 不要在“未知状态基线”上直接启动大迭代

## 3. 先定义本轮的成功标准

不要只写“把功能做出来”，要先写完成标准。

至少包括：

- 哪个行为会变好
- 哪些行为必须保持不变
- 哪些测试会新增
- 哪些现有测试必须继续通过
- 哪些运行态信号必须保持一致
- 哪些文档或 runbook 需要同步

对于 `quant-lab`，建议至少覆盖下列一种成功标准：

- 回测结果可信度更高
- 审批与执行门禁更硬
- 组合/单标行为更一致
- CLI / Service / Dashboard 输出更一致
- preflight / reconcile / submit gate 更可解释

## 4. 先定义本轮会碰到哪条主链路

一轮迭代开始前，必须先确认影响的是哪条链路：

- 数据采集链路
- 回测与报表链路
- 研究治理链路
- demo/live 执行链路
- 服务 API 与 dashboard 链路

如果跨多条链路，必须先写出为什么必须联动修改。

默认不要在一轮里同时大改：

- `backtest/engine`
- `execution/planner`
- `execution/strategy_router`
- `service/monitor`

如果必须联动改，先把共享决策源放在最前面，避免每层都各改一版逻辑。

## 5. 先定义本轮必须复跑的黄金路径

每轮迭代开始前，要先挑固定的回归路径，避免改完以后才临时找验证样本。

建议至少固定这些黄金路径：

- 单标回测
- 组合回测
- routed backtest
- demo preflight
- demo reconcile
- service API 基本健康检查

仓库内建议统一入口：

```powershell
.\.venv\Scripts\python.exe scripts/run_golden_regression.py
```

如果本轮动到了执行、审批或路由，至少要把 “plan-only -> blocked -> ready” 三类状态都覆盖到。

## 6. 先定义回滚策略和止损点

大于单文件修复的迭代，都应该先写回滚策略。

至少回答：

- 改坏以后，回滚单位是什么
- 哪些配置开关可以临时关掉新行为
- 哪些 artifact 或状态字段会受影响
- 哪些行为一旦出错必须立即停止推进

在这个仓库里，以下区域默认都属于高风险：

- runtime policy
- execution gate
- approval / route / bind
- artifact resolution
- runtime snapshot / heartbeat / alert 解释

## 7. 先确认是否需要同步运维材料

有些迭代改的不只是代码，而是运维方式。

如果本轮涉及以下任一内容，文档和 runbook 必须同步更新：

- preflight 输出变化
- reconcile 逻辑变化
- submit gate 行为变化
- 告警触发条件变化
- live/demo 切换规则变化
- Stage 0 rollout 规则变化

至少要回看：

- [docs/runtime_hard_constraints_2026-03-30.md](docs/runtime_hard_constraints_2026-03-30.md)
- [docs/live_rollout_stage0_runbook_2026-03-30.md](docs/live_rollout_stage0_runbook_2026-03-30.md)
- [docs/research_governance_workflow_2026-03-28.md](docs/research_governance_workflow_2026-03-28.md)

## 8. 先决定这轮迭代是“内部产品增强”还是“产品化准备”

不要把“工程增强”和“产品化落地”混为一谈。

启动前先判断本轮属于哪类：

- 内部工程改进
- 内部值守产品增强
- Stage 0 live 落地准备
- 长期产品化能力补齐

如果属于最后两类，建议同步阅读：

- [docs/productization_gap_assessment_2026-03-31.md](docs/productization_gap_assessment_2026-03-31.md)

## 9. 迭代启动前的固定动作

建议每次正式启动一轮迭代前，固定按这个顺序走：

1. 回看本文
2. 回看 `change_preflight_checklist`
3. 记录当前基线和风险
4. 写一行目标和一行非目标
5. 写黄金路径与验收标准
6. 写回滚策略
7. 标记涉及的主链路
8. 再开始真正实现

## 10. 默认结论

如果你在“直接开始做功能”和“先收口目标、锁定基线、定义回归路径”之间犹豫，默认选择后者。

对于 `quant-lab` 这种已经拥有研究、审批、执行和服务主干的仓库，真正昂贵的不是少写一次代码，而是：

- 一轮里同时碰太多链路
- 迭代目标不收口
- 没有黄金路径就开始改
- 改动上线后没有回滚和解释路径
