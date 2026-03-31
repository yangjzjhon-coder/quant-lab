# Quant-Lab 2026-03-31 修改计划

这份文档用于固定 `2026-03-31` 时点对 `quant-lab` 的修改与迭代路径。

目标不是一次性把所有事情做完，而是把当前已经铺开的改动面收口成可执行、可验证、可回滚的路线。

开始执行本计划前，先读：

- [docs/change_preflight_checklist_2026-03-31.md](docs/change_preflight_checklist_2026-03-31.md)
- [docs/iteration_readiness_checklist_2026-03-31.md](docs/iteration_readiness_checklist_2026-03-31.md)
- [docs/productization_gap_assessment_2026-03-31.md](docs/productization_gap_assessment_2026-03-31.md)

## 1. 当前判断

截至 `2026-03-31`，项目处于一次跨层重构的中段，不适合继续发散加新能力。

当前优先级不是“继续扩”，而是：

1. 先把本轮大改收成稳定基线
2. 再做主题明确的稳定性迭代
3. 再推进 Stage 0 live rollout
4. 再进入内部产品化

默认不建议：

- 继续横向扩策略族
- 直接推进多账户或多路由 live
- 现在就按外部 SaaS 的目标做架构

## 2. 第一阶段：收口当前重构

目标：

- 把当前大改从“进行中”收成“可解释、可测试、可继续迭代的基线”

本阶段优先事项：

- 修掉当前阻塞测试基线的红点
- 确认共享 runtime 口径是否真正统一
- 确认 `preflight / reconcile / submit gate / rollout policy` 只渲染共享结论
- 确认 routed backtest、service dashboard、client dashboard 的输入产物一致
- 把修改前、迭代前、产品化评估文档链固定下来

本阶段非目标：

- 新策略族扩展
- 多账户 live
- router-based live
- 大规模 UI 重构

当前执行动作：

- 修复 `research-routed-backtest` 当前已观测到的基线失败

## 3. 第二阶段：稳定性迭代

目标：

- 把“能跑”推进到“可信”

重点：

- 固定黄金回归路径
- 补高风险测试
- 统一 artifact 与状态解释
- 检查单标与组合模式是否真正共用规则栈
- 检查 CLI / Service / Dashboard 是否只适配共享决策源

本阶段通过标准：

- 测试基线稳定
- 高风险链路有回归覆盖
- 关键运行态输出口径一致

## 4. 第三阶段：Stage 0 live rollout 准备

目标：

- 从 demo 观察系统推进到最小 live 面

范围冻结：

- 1 account
- 1 symbol
- 1 approved candidate
- 1 signal/execution bar combination

执行原则：

- 严格遵守 [docs/live_rollout_stage0_runbook_2026-03-30.md](docs/live_rollout_stage0_runbook_2026-03-30.md)
- 路由关闭
- allow_order_placement 受控开启
- 所有 live 前置检查必须可解释、可阻断、可回滚

## 5. 第四阶段：内部产品化

目标：

- 让系统成为可长期值守的内部产品

重点：

- 发布与版本管理
- 环境与密钥治理
- 可观测性与运维能力
- 执行安全与恢复能力
- 数据与结果可复现
- 数据库演进与备份恢复
- operator 视角的控制台体验

## 6. 默认执行顺序

每轮执行时，默认遵守这个顺序：

1. 先修红基线
2. 再收口跨层改动
3. 再做稳定性主题迭代
4. 再做 Stage 0 live 准备
5. 最后再补产品化壳层

## 7. 当前这轮的落地动作

当前实际执行从第一阶段开始，优先做：

1. 把 `2026-03-31` 修改计划落到仓库
2. 修 routed backtest 产物与 dashboard 输入不一致的问题
3. 补回归测试，防止同类问题再次进入基线
4. 回跑相关测试，确认当前重构继续可收口

截至当前这轮，已经完成的收口项包括：

- routed backtest 输出补齐基础行情字段，修复 dashboard 市场图输入缺口
- 增加 routed backtest 回归测试，固定这条 artifact 合同
- 抽出共享 execution loop 状态聚合逻辑，消除 CLI 本地优先级分支
- 增加黄金回归入口 `scripts/run_golden_regression.py`
- 统一 Python 侧 autotrade 状态标签来源，避免 `client_ops` / `demo_runtime` 各自解释
- 客户端 dashboard 改为优先消费共享 `headline_summary` 与事件标签，减少前端本地状态映射漂移
- Service dashboard 改为优先消费 `dashboard_summary` 与后端序列化标签，减少页面本地状态翻译分叉
- 统一 Python 侧 side/action label helper 来源，减少 runtime / client payload 的重复解释函数
- 抽出双端 dashboard 共用 JS 基础 helper，减少 `client_dashboard` / `dashboard` 的前端工具层重复
- 为双端 dashboard 增加共享前端 helper 单测，固定页面基础脚本注入合同
- 抽出双端 dashboard 共用 `renderVisualReports(...)` 渲染函数，减少回测报表预览逻辑重复
- 抽出双端 dashboard 共用 `requestJson(...)` 请求 helper，减少前端 JSON 请求与错误处理重复
- 抽出双端 dashboard 共用 `formatMetricLines(...)` 报表指标格式化 helper，减少报表摘要格式化重复
- 收口 `client_dashboard` 内部重复的请求失败与产物刷新分支，减少页面内脚本重复
- 移除 `client_ops` 内对 client summary/status builder 的薄封装，改为直接消费 `demo_runtime` 共享实现，并让相关测试直接绑定共享构建器
- 移除前端页面里对 `requestJson(...)` 的 `req / fetchJson / postJson` 薄封装，统一直接调用共享请求 helper，继续压缩 dashboard 语义漂移面
- 将 `demo_visuals` 历史可视化 payload 与 heartbeat 事件序列化迁入 `demo_runtime`，让 `client_ops` 进一步退回纯编排层
- 移除 `demo_runtime` 与 `client_dashboard` 中已失效的本地 side/action 标签包装，页面事件流直接消费序列化标签
- 让 `monitor` / `project tasks` 复用共享 datetime、alert、heartbeat 序列化 helper，并补齐 UTC 时间戳契约回归
- 让 `preflight.execution_loop.latest_heartbeat` 直接携带 `status_label`，并让 runtime/client 汇总优先消费序列化标签
- 新增共享 `src/quant_lab/service/serialization.py`，下沉 UTC datetime 序列化 helper，并让 `demo_runtime / monitor / project_ops / research_ops` 复用同一实现，避免继续把时间语义绑在单一 runtime 模块里，同时规避 `demo_runtime <-> research_ops` 新循环依赖。
- 为 research workflow service API 补齐 UTC 时间戳契约回归，覆盖 `task / candidate / evaluation_report / approval / overview` 输出，固定所有研究流程核心响应统一返回 `+00:00` 时间格式。
- 继续收口 `monitor` 内剩余 `isoformat()` 直出点，让 monitor heartbeat 明细、report stale 告警文案、artifact catalog 的 `modified_at` 统一复用共享 UTC 序列化，并补齐对应 service monitor 回归断言。
