from __future__ import annotations

import html as html_lib

from fastapi.responses import HTMLResponse

from quant_lab.config import AppConfig, configured_symbols


def render_client_dashboard(config: AppConfig) -> HTMLResponse:
    symbols = configured_symbols(config)
    symbol_label = " / ".join(symbols)
    mode_label = "组合模式" if len(symbols) > 1 else "单标的模式"
    strategy_label = config.strategy.name

    html = """<!doctype html>
<html lang="zh-CN">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>quant-lab 本地中文客户端</title>
  <style>
    :root{--bg:#f4efe7;--panel:#fffdf9;--line:rgba(20,33,45,.12);--text:#17212b;--muted:#687784;--accent:#0b7285;--ok:#198754;--warn:#c76b00;--danger:#c2410c}
    *{box-sizing:border-box}body{margin:0;padding:24px;background:linear-gradient(180deg,#f2ede4 0%,#faf8f4 360px);color:var(--text);font:14px/1.6 "Microsoft YaHei UI","PingFang SC","Segoe UI",sans-serif}
    main{max-width:1440px;margin:0 auto;display:grid;gap:16px}section{background:var(--panel);border:1px solid var(--line);border-radius:20px;padding:18px;box-shadow:0 18px 42px rgba(20,33,45,.06)}
    h1,h2,h3{margin:0 0 10px}h1{font-size:30px}.muted,.hint,small{color:var(--muted)}a{color:var(--accent)}code{padding:2px 8px;border-radius:999px;background:#edf6f8;font-size:12px}
    .hero,.dual,.cards,.actions,.list,.feed{display:grid;gap:12px}.hero{grid-template-columns:minmax(0,1.4fr) minmax(320px,.8fr)}.dual{grid-template-columns:repeat(2,minmax(0,1fr))}.cards{grid-template-columns:repeat(auto-fit,minmax(160px,1fr))}
    .card,.item,.event{background:rgba(255,255,255,.75);border:1px solid var(--line);border-radius:16px;padding:14px}.value{font-size:24px;font-weight:700}.ok{color:var(--ok)}.warn{color:var(--warn)}.danger{color:var(--danger)}
    .pill{display:inline-flex;align-items:center;padding:7px 12px;border-radius:999px;font-size:12px;font-weight:700}.pill-ok{background:#e7f7ef;color:var(--ok)}.pill-warn{background:#fff3df;color:var(--warn)}.pill-danger{background:#fdebe5;color:var(--danger)}.pill-neutral{background:#eef2f5;color:#4d5c68}
    .row{display:flex;gap:12px;align-items:center;justify-content:space-between;flex-wrap:wrap}.actions{grid-template-columns:1fr 1fr auto}.check{display:flex;align-items:center;gap:8px}.action-buttons{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px}
    input[type="text"],textarea{width:100%;padding:11px 12px;border:1px solid var(--line);border-radius:12px;background:#fff;color:#111;font:inherit}textarea{min-height:88px;resize:vertical}
    button{min-height:44px;padding:10px 14px;border:0;border-radius:12px;background:linear-gradient(135deg,#0b7285,#1098ad);color:#fff;font-weight:700;cursor:pointer}button.alt{background:linear-gradient(135deg,#c46a00,#ef7d00)}button.warn{background:linear-gradient(135deg,#b45309,#d97706)}button.danger{background:linear-gradient(135deg,#b93815,#dc5b2a)}button:disabled{opacity:.55;cursor:wait}
    .list,.feed{max-height:380px;overflow:auto}.item,.event{white-space:pre-wrap;word-break:break-word}.mono{font-family:"IBM Plex Mono","Consolas",monospace}pre{margin:0;padding:16px;border-radius:16px;border:1px solid #22303b;background:#12202a;color:#e8f1f2;max-height:420px;overflow:auto;font:12px/1.55 "IBM Plex Mono","Consolas",monospace}
    svg{width:100%;height:290px;display:block;border-radius:16px;border:1px solid var(--line);background:linear-gradient(180deg,#f7fbfc,#fffdf9)}.empty{padding:18px;border:1px dashed var(--line);border-radius:16px;text-align:center;color:var(--muted)}.footer{text-align:right;color:var(--muted);font-size:12px}
    @media (max-width:1120px){.hero,.dual,.actions{grid-template-columns:1fr}}
  </style>
</head>
<body>
  <main>
    <section>
      <div class="hero">
        <div>
          <div class="muted">quant-lab 本地中文客户端</div>
          <h1>__SYMBOLS__</h1>
          <div class="hint">策略 <code>__STRATEGY__</code>，当前为 <strong>__MODE__</strong>。这个页面用于查看模拟盘状态、触发本地检查动作、观察最近的 demo-loop 运行结果。</div>
          <div class="row" style="margin-top:12px">
            <a href="/" target="_blank" rel="noreferrer">打开运行总览</a>
            <a href="/reports/backtest" target="_blank" rel="noreferrer">打开回测报表</a>
            <a href="/reports/sweep" target="_blank" rel="noreferrer">打开参数扫描</a>
          </div>
        </div>
        <div class="cards">
          <div class="card"><small>策略</small><div class="value">__STRATEGY__</div></div>
          <div class="card"><small>运行模式</small><div class="value">__MODE__</div></div>
          <div class="card"><small>数据来源</small><div class="value" id="meta-source">--</div></div>
          <div class="card"><small>最后刷新</small><div class="value" id="meta-updated">--</div></div>
        </div>
      </div>
    </section>

    <section>
      <div class="row">
        <div><h2>执行总览</h2><div class="hint">先看这里，判断当前能不能自动下单、有没有阻塞、最近一轮循环处于什么状态。</div></div>
        <span id="headline-pill" class="pill pill-neutral">等待数据</span>
      </div>
      <div class="cards" style="margin-top:12px">
        <div class="card"><small>自动交易状态</small><div class="value" id="headline-title">加载中</div><div class="hint" id="headline-subtitle">--</div></div>
        <div class="card"><small>允许提交</small><div class="value" id="headline-submit">--</div><div class="hint" id="headline-submit-note">--</div></div>
        <div class="card"><small>本轮是否有动作</small><div class="value" id="headline-actionable">--</div><div class="hint" id="headline-actionable-note">--</div></div>
        <div class="card"><small>最近循环状态</small><div class="value" id="headline-loop">--</div><div class="hint" id="headline-loop-note">--</div></div>
      </div>
    </section>

    <section>
      <h2>操作台</h2>
      <div class="hint">这里只会调用本地接口，不会因为打开页面就自动下单。</div>
      <div class="actions" style="margin-top:12px">
        <label>确认字符串<input id="confirm-input" type="text" placeholder="OKX_DEMO"></label>
        <label>告警测试内容<textarea id="alert-message">quant-lab 本地客户端测试</textarea></label>
        <div class="card">
          <label class="check"><input id="rearm-stop" type="checkbox"> 杠杆对齐时允许临时撤止损</label>
          <label class="check"><input id="auto-refresh" type="checkbox" checked> 每 30 秒自动刷新</label>
        </div>
      </div>
      <div class="action-buttons" style="margin-top:12px">
        <button id="btn-refresh">刷新快照</button>
        <button id="btn-reconcile" class="alt">重新对账</button>
        <button id="btn-align-dry" class="warn">杠杆 Dry-Run</button>
        <button id="btn-align-apply" class="danger">应用杠杆对齐</button>
        <button id="btn-alert" class="alt">发送测试告警</button>
      </div>
    </section>

    <section class="dual">
      <div>
        <h2>执行准备</h2>
        <div class="cards" style="margin-top:12px">
          <div class="card"><small>Demo 通道</small><div class="value" id="check-demo">--</div><div class="hint" id="check-demo-note">--</div></div>
          <div class="card"><small>杠杆对齐</small><div class="value" id="check-leverage">--</div><div class="hint" id="check-leverage-note">--</div></div>
          <div class="card"><small>仓位对齐</small><div class="value" id="check-size">--</div><div class="hint" id="check-size-note">--</div></div>
          <div class="card"><small>保护止损</small><div class="value" id="check-stop">--</div><div class="hint" id="check-stop-note">--</div></div>
        </div>
        <div class="list" id="plan-summary" style="margin-top:12px"><div class="empty">等待数据</div></div>
      </div>
      <div>
        <h2>风险与阻塞</h2>
        <div class="hint">这里汇总阻塞原因、账户异常、实时拉取失败等信息。</div>
        <div class="list" id="warning-list" style="margin-top:12px"><div class="empty">等待数据</div></div>
      </div>
    </section>

    <section class="dual">
      <div>
        <h2>交易所状态</h2>
        <div class="list" id="exchange-list" style="margin-top:12px"><div class="empty">等待数据</div></div>
      </div>
      <div>
        <h2 id="symbols-title">标的状态</h2>
        <div class="hint" id="symbols-note">等待数据</div>
        <div class="list" id="symbol-list" style="margin-top:12px"><div class="empty">等待数据</div></div>
      </div>
    </section>

    <section class="dual">
      <div>
        <div class="row">
          <div><h2>模拟盘运行历史</h2><div class="hint" id="history-note">等待数据</div></div>
          <small id="history-updated">--</small>
        </div>
        <div class="cards" style="margin-top:12px">
          <div class="card"><small>总循环数</small><div class="value" id="history-cycles">--</div></div>
          <div class="card"><small>已提交次数</small><div class="value" id="history-submitted">--</div></div>
          <div class="card"><small>提交率</small><div class="value" id="history-rate">--</div></div>
          <div class="card"><small>最近状态</small><div class="value" id="history-status">--</div></div>
        </div>
        <div style="margin-top:12px">
          <small id="chart-summary">--</small>
          <svg id="history-chart" viewBox="0 0 760 290" preserveAspectRatio="none"></svg>
          <div class="hint" id="chart-note" style="margin-top:8px">--</div>
        </div>
      </div>
      <div>
        <h2>最近动作与告警</h2>
        <div class="feed" id="event-feed" style="margin-top:12px"><div class="empty">等待数据</div></div>
        <div class="feed" id="alert-feed" style="margin-top:12px"><div class="empty">等待数据</div></div>
      </div>
    </section>

    <section>
      <div class="row"><div><h2>原始返回</h2><div class="hint">保留接口原始 JSON，方便排查问题。</div></div><small id="result-stamp">--</small></div>
      <pre id="raw-json">等待数据</pre>
    </section>

    <div class="footer">页面只负责查看状态和触发本地接口；真正的自动下单只会在后台 demo-loop 中发生。</div>
  </main>
  <script>
    const AUTO_REFRESH_MS=30000; let timer=null;
    const $=(id)=>document.getElementById(id), O=(v)=>v&&typeof v==='object'&&!Array.isArray(v)?v:{}, A=(v)=>Array.isArray(v)?v:[], N=(v,d=null)=>v===null||v===undefined||v===''||Number.isNaN(Number(v))?d:Number(v), S=(v,d='--')=>v===null||v===undefined||v===''?d:String(v);
    const t=(v)=>{ if(!v) return '--'; const d=new Date(v); return Number.isNaN(d.getTime())?String(v):d.toLocaleString('zh-CN',{hour12:false}); };
    const f=(v,d=2)=>{ const n=N(v,null); return n===null?'--':n.toLocaleString('zh-CN',{minimumFractionDigits:d,maximumFractionDigits:d}); };
    const pct=(v)=>{ const n=N(v,null); return n===null?'--':`${f(n,2)}%`; };
    const side=(v)=>{ const n=N(v,null); return n===null?'--':n>0?'做多':n<0?'做空':'空仓'; };
    const boolText=(v,y='是',n='否',u='未知')=>v===true?y:v===false?n:u;
    const loopStatus=(v)=>({submitted:'已提交',duplicate:'重复已跳过',idle:'无动作',warning:'警告',error:'错误',plan_only:'仅规划',ok:'正常',missing:'未启动'})[String(v||'').trim()]||S(v);
    const demoMode=(v)=>({submit_ready:'可提交',submit_blocked:'已阻塞',plan_only:'仅规划',unknown:'未知'})[String(v||'').trim()]||S(v);
    const sourceText=(v)=>({live_okx:'OKX 实时',cached_local_state:'本地缓存'})[String(v||'').trim()]||S(v);
    const reason=(v)=>{ const raw=S(v,'').trim(); const map={'missing OKX_API_KEY':'缺少 OKX_API_KEY','missing OKX_SECRET_KEY':'缺少 OKX_SECRET_KEY','missing OKX_PASSPHRASE':'缺少 OKX_PASSPHRASE','okx.use_demo=false':'当前不是 OKX Demo 模式','trading.allow_order_placement=false':'当前未开启自动下单开关'}; if(map[raw]) return map[raw]; if(raw.startsWith('execution approval: ')) return `审批门禁未通过：${raw.slice(20)}`; return raw||'未提供原因'; };
    const esc=(v)=>String(v??'').replaceAll('&','&amp;').replaceAll('<','&lt;').replaceAll('>','&gt;').replaceAll('"','&quot;').replaceAll("'",'&#39;');
    function metric(id,val,kind,note){ const el=$(id); el.textContent=val; el.className=`value ${kind}`; const noteEl=$(`${id}-note`); if(noteEl) noteEl.textContent=note||'--'; }
    function portfolio(snapshot){ const r=O(snapshot?.reconcile), s=O(snapshot?.demo_visuals?.summary); return String(r.mode||s.mode||'').toLowerCase()==='portfolio'; }
    function loop(snapshot){ return O(snapshot?.preflight?.execution_loop?.latest_heartbeat); }
    function checksAggregate(r){ const states=Object.values(O(r?.symbol_states)); const total=states.length, active=states.filter((x)=>N(O(x?.position).side,0)!==0&&(N(O(x?.position).contracts,0)||0)>0).length; return {total,active,lev:states.filter((x)=>O(x?.checks).leverage_match===true).length,size:states.filter((x)=>O(x?.checks).size_match===true).length,stop:states.filter((x)=>O(x?.checks).protective_stop_ready===true).length}; }
    function renderHeadline(snapshot){ const p=O(snapshot?.preflight?.demo_trading), a=O(snapshot?.autotrade_status), h=loop(snapshot), can=p.ready===true, act=a.will_submit_now===true, level=can?(act?'ok':'warn'):'danger'; $('headline-pill').className=`pill ${level==='ok'?'pill-ok':level==='danger'?'pill-danger':'pill-warn'}`; $('headline-pill').textContent=can?(act?'可自动执行':'通道已开，等待信号'):'当前不可提交'; $('headline-title').textContent=S(a.headline,can?'自动交易已就绪':'自动交易未就绪'); $('headline-subtitle').textContent=`${demoMode(p.mode)} | 数据来源：${sourceText(snapshot?.snapshot_source)} | 最近时间：${t(h.created_at||snapshot?.demo_visuals?.summary?.last_event_time)}`; metric('headline-submit',can?'允许':'不允许',can?'ok':'danger',can?'当前配置允许向 OKX Demo 提交订单':'当前配置或门禁仍阻止提交'); metric('headline-actionable',act?'有动作':'无动作',act?'ok':'warn',act?'本轮存在可执行指令':'当前没有可执行动作或仍被阻塞'); metric('headline-loop',loopStatus(h.status||a.latest_loop_status||'missing'),h.status==='error'?'danger':'ok',h.status?`最近循环时间：${t(h.created_at)}`:'当前还没有 demo-loop 心跳'); }
    function renderChecks(snapshot){ const p=O(snapshot?.preflight?.demo_trading), r=O(snapshot?.reconcile); if(portfolio(snapshot)){ const g=checksAggregate(r); metric('check-demo',p.ready?'已打通':demoMode(p.mode),p.ready?'ok':'danger',p.ready?'可以向 OKX Demo 提交组合计划':'组合计划暂时还不能提交'); metric('check-leverage',`${g.lev}/${g.total}`,g.total>0&&g.lev===g.total?'ok':'warn','已对齐目标杠杆的标的数量'); metric('check-size',`${g.size}/${g.total}`,g.total>0&&g.size===g.total?'ok':'warn','已贴合目标仓位的标的数量'); metric('check-stop',g.active===0?'无持仓':`${g.stop}/${g.active}`,g.active===0||g.stop===g.active?'ok':'warn','已挂保护止损的持仓标的数量'); return; } const c=O(r?.checks); metric('check-demo',p.ready?'已打通':demoMode(p.mode),p.ready?'ok':'danger',p.ready?'可以提交到 OKX Demo':'当前仍是仅规划或受阻状态'); metric('check-leverage',boolText(c.leverage_match,'已对齐','未对齐'),'leverage_match' in c?(c.leverage_match?'ok':'warn'):'warn',c.leverage_match===true?'交易所杠杆与策略目标一致':'交易所杠杆还未对齐'); metric('check-size',boolText(c.size_match,'已对齐','未对齐'),'size_match' in c?(c.size_match?'ok':'warn'):'warn',c.size_match===true?'当前仓位已跟随目标仓位':'当前仓位与目标仓位仍有偏差'); metric('check-stop',boolText(c.protective_stop_ready,'已就绪','缺失'),'protective_stop_ready' in c?(c.protective_stop_ready?'ok':'danger'):'warn',c.protective_stop_ready===true?'保护止损已存在':'当前保护止损未就绪'); }
    function renderPlan(snapshot){ const r=O(snapshot?.reconcile), box=$('plan-summary'); if(portfolio(snapshot)){ const s=O(r?.summary), a=O(r?.account), items=[`账户权益：${f(a.total_equity,2)} ${S(a.currency,'USDT')}`,`可用权益：${f(a.available_equity,2)} ${S(a.currency,'USDT')}`,`组合标的数：${f(s.symbol_count,0)}`,`可执行标的数：${f(s.actionable_symbol_count,0)}`,`当前有持仓标的数：${f(s.active_position_symbol_count,0)}`,`单标的规划资金：${f(s.per_symbol_planning_equity,2)}`,`最近循环模式：组合模式`]; box.innerHTML=items.map((x)=>`<div class="item">${esc(x)}</div>`).join(''); return; } const a=O(r?.account), p=O(r?.position), sig=O(r?.signal), plan=O(r?.plan), items=[`账户权益：${f(a.total_equity,2)} ${S(a.currency,'USDT')}`,`可用权益：${f(a.available_equity,2)} ${S(a.currency,'USDT')}`,`当前持仓：${side(p.side)} | ${f(p.contracts,2)} 张`,`策略方向：${side(sig.desired_side)}`,`计划动作：${S(plan.action,'无')}`,`目标仓位：${f(plan.target_contracts,2)} 张`,`计划原因：${reason(plan.reason||'未提供原因')}`,`最新价格：${f(sig.latest_price??plan.latest_price,2)}`]; box.innerHTML=items.map((x)=>`<div class="item">${esc(x)}</div>`).join(''); }
    function renderWarnings(snapshot){ const items=[]; A(O(snapshot?.preflight?.demo_trading).reasons).forEach((x)=>items.push(reason(x))); A(snapshot?.reconcile?.warnings).forEach((x)=>items.push(reason(x))); A(snapshot?.autotrade_status?.blocking_reasons).forEach((x)=>items.push(reason(x))); if(snapshot?.live_error) items.push(`实时抓取失败：${snapshot.live_error}`); const uniq=[...new Set(items.filter(Boolean))]; $('warning-list').innerHTML=uniq.length?uniq.map((x)=>`<div class="item">${esc(x)}</div>`).join(''):'<div class="empty">当前没有明确的阻塞项</div>'; }
    function renderExchange(snapshot){ const r=O(snapshot?.reconcile), p=O(snapshot?.preflight), x=O(r.exchange), lev=O(x.leverage), stop=O(x.protection_stop), po=O(x.pending_orders), pa=O(x.pending_algo_orders), conn=O(p.okx_connectivity); const items=[`数据来源：${sourceText(snapshot?.snapshot_source)}`,`账户模式：${S(r?.account?.account_mode||r?.position?.position_mode,'--')}`,`普通挂单数：${f(po.count,0)}`,`条件单数：${f(pa.count,0)}`,`杠杆值：${A(lev.values).length?A(lev.values).join(', '):'--'}`,`保护止损：${boolText(stop.ready,'已就绪','未就绪')}`,`OKX Profile：${S(conn.profile,'--')}`,`代理：${S(conn.proxy_url,'未配置')}`,`出口 IP：${S(conn.egress_ip,'--')}`]; A(conn.notes).forEach((x)=>items.push(`连接提示：${reason(x)}`)); if(snapshot?.live_error) items.push(`实时错误：${snapshot.live_error}`); $('exchange-list').innerHTML=items.map((x)=>`<div class="item">${esc(x)}</div>`).join(''); }
    function renderSymbols(snapshot){ const r=O(snapshot?.reconcile), box=$('symbol-list'); if(portfolio(snapshot)){ $('symbols-title').textContent='组合标的状态'; $('symbols-note').textContent='逐个标的展示当前持仓、目标方向、目标仓位和关键检查。'; const states=Object.entries(O(r.symbol_states)); box.innerHTML=states.length?states.map(([symbol,payload])=>{ const p=O(payload?.position), sig=O(payload?.signal), plan=O(payload?.plan), c=O(payload?.checks); return `<div class="item"><strong>${esc(symbol)}</strong>\n当前方向：${esc(side(p.side))} | 策略方向：${esc(side(sig.desired_side))}\n当前仓位：${esc(f(p.contracts,2))} 张 | 目标仓位：${esc(f(plan.target_contracts,2))} 张\n计划动作：${esc(S(plan.action,'无'))}\n杠杆：${esc(boolText(c.leverage_match,'已对齐','未对齐'))} | 仓位：${esc(boolText(c.size_match,'已对齐','未对齐'))} | 止损：${esc(boolText(c.protective_stop_ready,'已就绪','未就绪'))}</div>`; }).join(''):'<div class="empty">组合模式下暂无标的状态</div>'; return; } $('symbols-title').textContent='当前标的状态'; $('symbols-note').textContent='展示当前标的的持仓方向、策略目标方向和目标仓位。'; const p=O(r.position), sig=O(r.signal), plan=O(r.plan); box.innerHTML=`<div class="item"><strong>${esc(S(r.instrument,'--'))}</strong>\n当前方向：${esc(side(p.side))} | 策略方向：${esc(side(sig.desired_side))}\n当前仓位：${esc(f(p.contracts,2))} 张 | 目标仓位：${esc(f(plan.target_contracts,2))} 张\n计划动作：${esc(S(plan.action,'无'))}\n执行原因：${esc(reason(plan.reason||'未提供原因'))}</div>`; }
    function chart(points, mode){ const svg=$('history-chart'); const usable=A(points).filter((x)=>x&&x.target_contracts!==null&&x.target_contracts!==undefined); if(!usable.length){ svg.innerHTML='<text x="50%" y="50%" text-anchor="middle" fill="#687784" font-size="16">暂无足够的 demo-loop 历史</text>'; $('chart-summary').textContent='--'; $('chart-note').textContent='等 demo-loop 再跑几轮后，这里会出现折线。'; return; } const W=760,H=290,L=48,R=18,U=18,B=28,vals=[...usable.map((x)=>Number(x.target_contracts)),...usable.map((x)=>N(x.current_contracts,null)).filter((x)=>x!==null)],min=Math.min(...vals),max=Math.max(...vals),span=Math.max(max-min,1),step=usable.length>1?(W-L-R)/(usable.length-1):0,X=(i)=>L+step*i,Y=(v)=>H-B-(((Number(v)-min)/span)*(H-U-B)); const grid=Array.from({length:4},(_,i)=>{ const val=min+(span*i/3),y=Y(val); return `<line x1="${L}" y1="${y.toFixed(2)}" x2="${W-R}" y2="${y.toFixed(2)}" stroke="rgba(23,33,43,.08)" stroke-dasharray="4 6"></line><text x="${L-8}" y="${(y+4).toFixed(2)}" text-anchor="end" fill="#687784" font-size="11">${f(val,2)}</text>`; }).join(''); const path=usable.map((x,i)=>`${i===0?'M':'L'} ${X(i).toFixed(2)} ${Y(x.target_contracts).toFixed(2)}`).join(' '); const live=usable.filter((x)=>x.current_contracts!==null&&x.current_contracts!==undefined).map((x,i)=>`${i===0?'M':'L'} ${X(i).toFixed(2)} ${Y(x.current_contracts).toFixed(2)}`).join(' '); const dots=usable.map((x,i)=>`<circle cx="${X(i).toFixed(2)}" cy="${Y(x.target_contracts).toFixed(2)}" r="4.5" fill="${x.status==='submitted'?'#ef7d00':x.status==='error'?'#c2410c':'#0b7285'}"></circle>`).join(''); svg.innerHTML=`${grid}<path d="${path}" fill="none" stroke="#0b7285" stroke-width="3"></path>${live?`<path d="${live}" fill="none" stroke="#94a3b8" stroke-width="2" stroke-dasharray="8 6"></path>`:''}${dots}`; $('chart-summary').textContent=mode==='portfolio'?'实线表示最近各轮中“可执行标的数量”，虚线表示“当前持仓标的数量”。':'实线表示目标仓位，虚线表示记录到的当前仓位。'; $('chart-note').textContent=`最近绘制点数：${usable.length}`; }
    function renderHistory(snapshot){ const s=O(snapshot?.demo_visuals?.summary), c=O(snapshot?.demo_visuals?.chart), mode=String(s.mode||c.mode||(portfolio(snapshot)?'portfolio':'single')).toLowerCase(); $('history-note').textContent=mode==='portfolio'?'组合模式下，这里展示每轮可执行标的数、最近循环结果和告警。':'单标的模式下，这里展示目标仓位变化、最近循环结果和告警。'; $('history-updated').textContent=t(s.last_event_time); $('history-cycles').textContent=f(s.total_cycles,0); $('history-submitted').textContent=f(s.submitted_count,0); $('history-rate').textContent=pct(s.submission_rate_pct); $('history-status').textContent=S(s.last_status_label,'--'); chart(c.points,mode); const events=A(snapshot?.demo_visuals?.recent_events), alerts=A(snapshot?.demo_visuals?.recent_alerts); $('event-feed').innerHTML=events.length?events.map((x)=>`<div class="event"><strong>第 ${esc(S(x.cycle,'--'))} 轮 | ${esc(S(x.action,'--'))}</strong>\n状态：${esc(loopStatus(x.status))} | 时间：${esc(t(x.created_at))}\n当前方向：${esc(side(x.current_side))} | 策略方向：${esc(side(x.desired_side))}\n目标仓位：${esc(f(x.target_contracts,2))} | 当前仓位：${esc(f(x.current_contracts,2))}\n响应数：${esc(f(x.response_count,0))} | 警告数：${esc(f(x.warning_count,0))}</div>`).join(''):'<div class="empty">最近没有 demo-loop 事件</div>'; $('alert-feed').innerHTML=alerts.length?alerts.map((x)=>`<div class="event"><strong>${esc(S(x.title,x.event_key||'--'))}</strong>\n时间：${esc(t(x.created_at))} | 渠道：${esc(S(x.channel,'--'))} | 状态：${esc(S(x.status,'--'))}\n${esc(S(x.message,'--'))}</div>`).join(''):'<div class="empty">最近没有告警记录</div>'; }
    function renderRaw(payload){ $('result-stamp').textContent=t(new Date().toISOString()); $('raw-json').textContent=JSON.stringify(payload,null,2); }
    function meta(snapshot){ $('meta-source').textContent=sourceText(snapshot?.snapshot_source); $('meta-updated').textContent=t(new Date().toISOString()); }
    function busy(v){ ['btn-refresh','btn-reconcile','btn-align-dry','btn-align-apply','btn-alert'].forEach((id)=>{ const el=$(id); if(el) el.disabled=v; }); }
    function alignBody(apply){ return {apply,confirm:$('confirm-input').value||'',rearm_protective_stop:$('rearm-stop').checked}; }
    function auto(){ if(timer) clearInterval(timer); timer=$('auto-refresh').checked?setInterval(()=>load(),AUTO_REFRESH_MS):null; }
    async function req(url,opt){ const res=await fetch(url,opt), txt=await res.text(); let data={}; try{ data=txt?JSON.parse(txt):{}; }catch{ data={raw:txt}; } if(!res.ok) throw new Error(data.detail||txt||`Request failed: ${url}`); return data; }
    async function load(url='/client/snapshot',opt){ busy(true); try{ const payload=await req(url,opt), snapshot=O(payload.snapshot||payload); meta(snapshot); renderHeadline(snapshot); renderChecks(snapshot); renderPlan(snapshot); renderWarnings(snapshot); renderExchange(snapshot); renderSymbols(snapshot); renderHistory(snapshot); renderRaw(payload); }catch(err){ $('meta-updated').textContent='读取失败'; $('headline-title').textContent='客户端读取失败'; $('headline-subtitle').textContent=String(err); $('headline-pill').className='pill pill-danger'; $('headline-pill').textContent='请求失败'; $('raw-json').textContent=JSON.stringify({error:String(err)},null,2); }finally{ busy(false); } }
    $('btn-refresh').addEventListener('click',()=>load());
    $('btn-reconcile').addEventListener('click',()=>load('/client/reconcile',{method:'POST'}));
    $('btn-align-dry').addEventListener('click',()=>load('/client/align-leverage',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(alignBody(false))}));
    $('btn-align-apply').addEventListener('click',()=>load('/client/align-leverage',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(alignBody(true))}));
    $('btn-alert').addEventListener('click',()=>load('/client/alert-test',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({message:$('alert-message').value||'quant-lab 本地客户端测试'})}));
    $('auto-refresh').addEventListener('change',auto); auto(); load();
  </script>
</body>
</html>"""

    for old, new in {
        "__SYMBOLS__": html_lib.escape(symbol_label),
        "__STRATEGY__": html_lib.escape(strategy_label),
        "__MODE__": html_lib.escape(mode_label),
    }.items():
        html = html.replace(old, new)
    return HTMLResponse(html)
