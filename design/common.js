// 设计样张共用：读取线上的 data.json / themes.json，整理成 4 款样张都用的同一份内容
// 只做展示：行情用 data.json 里的快照，不实时刷新、不跳转
window.MW = (() => {
  const esc = s => String(s ?? '').replace(/[&<>"']/g, c => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;' }[c]));
  const pad = n => String(n).padStart(2, '0');
  const cst = ts => new Date((ts + 8 * 3600) * 1000);
  const hm = ts => { const d = cst(ts); return `${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}`; };
  const cls = v => v > 0 ? 'up' : v < 0 ? 'down' : 'flat';
  const sign = v => (v > 0 ? '+' : '') + v;
  const pct = (v, d = 2) => v == null ? '—' : sign(v.toFixed(d)) + '%';
  const fmtP = v => v == null ? '—' : Number(v).toLocaleString('en-US', { maximumFractionDigits: Math.abs(v) >= 10 ? 2 : 4 });
  const MKT = { 105: 'US', 106: 'US', 107: 'US', 116: 'HK', 0: 'CN', 1: 'CN', 176: 'JP', 177: 'KR', 178: 'TW' };
  const MKT_NAME = { US: '美', HK: '港', CN: 'A', JP: '日', KR: '韩', TW: '台' };
  const CUR = { US: '美元', HK: '港元', CN: '元', JP: '日元', KR: '韩元', TW: '新台币' };
  const mktOf = id => MKT[id.split('.')[0]] || '';
  const big = v => v == null ? '—' : Math.abs(v) >= 1e12 ? (v / 1e12).toFixed(2) + '万亿' : Math.abs(v) >= 1e8 ? (v / 1e8).toFixed(1) + '亿' : String(v);
  const cap = (v, id) => v == null ? '—' : big(v) + (CUR[mktOf(id)] || '');

  function usSession() {
    const et = new Date(new Date().toLocaleString('en-US', { timeZone: 'America/New_York' }));
    const d = et.getDay(), m = et.getHours() * 60 + et.getMinutes();
    if (d === 0 || d === 6) return '休市';
    if (m >= 570 && m < 960) return '美股交易中';
    if (m >= 240 && m < 570) return '美股盘前';
    if (m >= 960 && m < 1200) return '美股盘后';
    return '待美股盘前';
  }

  // 迷你走势：SVG polyline 点串
  function spark(pts, w = 100, h = 28) {
    if (!pts || pts.length < 2) return '';
    const vs = pts.map(p => p[1]), lo = Math.min(...vs), hi = Math.max(...vs), r = hi - lo || 1;
    return pts.map((p, i) => `${(i / (pts.length - 1) * w).toFixed(1)},${(h - (p[1] - lo) / r * (h - 2) - 1).toFixed(1)}`).join(' ');
  }

  async function load() {
    const [D, TH] = await Promise.all(['../data.json', '../themes.json'].map(f => fetch(f + '?t=' + Date.now()).then(r => r.json())));
    const Q = D.quotes || {};
    const NEWS = Object.fromEntries(D.news.map(n => [n.id, n]));
    const SRC = Object.fromEntries(D.sources.map(s => [s.id, s]));
    const FAC = Object.fromEntries(D.factors.map(f => [f.key, f.name]));
    const themePct = t => {
      if (t.quote) return Q[t.quote]?.pct ?? null;
      if (t.board) return Q[t.board]?.pct ?? null;
      const v = t.stocks.map(id => Q[id]?.pct).filter(x => x != null);
      return v.length ? v.reduce((a, b) => a + b, 0) / v.length : null;
    };
    const mk = id => D.markets.find(m => m.id === id);
    const g = TH.tabs.find(t => t.key === 'global');

    const macro = g.sections[0].items.map(it => ({ ...it, q: Q[it.id] || {}, pts: mk(it.id)?.pts }));
    const industry = g.sections[1].items.map(k => ({ key: k, ...TH.themes[k], pct: themePct(TH.themes[k]) }));

    // 今日异动：按「幅度 ÷ 阈值」挑最显著的 5 段，再按时间倒序
    const moves = [];
    D.markets.forEach(m => m.moves.forEach(mv => moves.push({ m, mv, news: (mv.news || []).map(x => NEWS[x.id]).filter(Boolean) })));
    moves.sort((a, b) => Math.abs(b.mv.chg) / b.m.th - Math.abs(a.mv.chg) / a.m.th);
    const topMoves = moves.slice(0, 5).sort((a, b) => b.mv.end - a.mv.end);

    const events = D.events.slice(0, 5).map(e => ({ ...e, refs: e.refs.map(id => NEWS[id]).filter(Boolean) }));

    const ai = TH.themes['ai-compute'];
    const aiTheme = { ...ai, pct: themePct(ai), rows: ai.stocks.map(id => ({ id, name: Q[id]?.name || TH.names[id], q: Q[id] || {}, mkt: MKT_NAME[mktOf(id)] }))
      .sort((a, b) => (b.q.pct ?? -1e9) - (a.q.pct ?? -1e9)) };

    const indices = ['1.000001', '100.HSI', '100.NDX', '100.SPX', '100.N225', '100.KS11'].map(mk).filter(Boolean);

    return { D, TH, Q, SRC, FAC, macro, industry, topMoves, events, aiTheme, indices, updated: D.updated.slice(5, 16).replace('T', ' ') };
  }

  return { load, esc, hm, cls, sign, pct, fmtP, cap, big, spark, usSession, mktOf };
})();
