/**
 * Запуски монет pump.fun (SOL-кривая, не mayhem) → кластеры создателей в сборщике переводов copy-trade-solana (tools/creator-clusters.js):
 * POST LAUNCHES_URL (http://10.156.0.18:7730/launches) пачками раз в LAUNCHES_FLUSH_MS (300). Дев — первый покупатель в транзакции создания (как в симе).
 * С 24.10 в запуске и создатель монеты (поле creator события) — сборщик склеивает его с девом, пока у создателя один покупатель.
 * LAUNCHES=0 — выключено. Боты спрашивают /launch?mint= (prior = 0 — новый кластер).
 * «Почерк» (бумажный режим, tools/pocherk.js сборщика): путь цены каждого такого запуска — состояние кривой (vsr SOL, vtr токенов) по последней сделке ≤ t0 + k·250 мс,
 * k = 0…144 (до 36 с; t0 — первая сделка монеты, как в симе); через PATHS_AFTER_MS (37 000) от первой сделки — POST PATHS_URL (…/paths). PATHS=0 — выключено.
 */
'use strict';
const {logger} = require('./common');
const ON = process.env.LAUNCHES !== '0';
const URL_ = process.env.LAUNCHES_URL || 'http://10.156.0.18:7730/launches';
const FLUSH_MS = parseInt(process.env.LAUNCHES_FLUSH_MS || '300');
const WSOL = 'So11111111111111111111111111111111111111112';
let buf = [];
const PATHS_ON = ON && process.env.PATHS !== '0';
const PATHS_URL = process.env.PATHS_URL || URL_.replace(/\/launches$/, '/paths');
const PATHS_AFTER_MS = parseInt(process.env.PATHS_AFTER_MS || '37000');
const track = new Map();   // монета → {t0, at (местное время начала), pts: [[dt, vsr, vtr]]}
const SP = {tracked: 0, sent: 0, errors: 0};
function trackStart(mint, t0) { if (PATHS_ON && !track.has(mint)) { track.set(mint, {t0, at: Date.now(), pts: []}); SP.tracked++; } }
function trackSwaps(r) {
    if (!track.size) return;
    for (const x of r.swaps) {
        const t = track.get(x.mint); if (!t || x.mayhem_mode || x.quote_mint) continue;
        const dt = r.ts - t.t0; if (dt < 0 || dt > 36500) continue;
        t.pts.push([dt, Number(x.virtual_sol_reserves) / 1e9, Number(x.virtual_token_reserves) / 1e6]);
    }
}
function buildPath(t) {
    const pts = t.pts.slice().sort((a, b) => a[0] - b[0]); const vs = [], vt = []; let k = 0;
    for (let g = 0; g <= 36000; g += 250) { while (k + 1 < pts.length && pts[k + 1][0] <= g) k++; vs.push(+pts[k][1].toFixed(9)); vt.push(Math.round(pts[k][2] * 1e3) / 1e3); }
    return {vs, vt};
}
const S = {creates: 0, sent: 0, noSwap: 0, other: 0, late: 0, errors: 0};
const pending = new Map();   // создание без покупки в той же транзакции: монета → время создания (дев — первый покупатель позже)
function onPumpBc(r) {
    if (!ON || !r || !r.create) return;
    S.creates++;
    const sw = r.swaps || [];
    const s = sw.find(x => x.mint && !x.mayhem_mode && (!x.quote_mint || x.quote_mint === WSOL));
    if (s && s.user) { buf.push({mint: s.mint, dev: s.user, creator: s.creator || null, t0: r.ts}); trackStart(s.mint, r.ts); trackSwaps(r); return; }
    if (!sw.length) { S.noSwap++; const m = r.tokens && Object.keys(r.tokens)[0]; if (m) pending.set(m, r.ts); return; }
    S.other++;   // mayhem / не SOL — бот их не торгует
}
/** первая покупка монеты, созданной без покупки дева: дев — этот покупатель (как в симе: первый своп монеты) */
function onSwaps(r) {
    if (!ON || !r || !r.swaps) return;
    if (PATHS_ON) trackSwaps(r);
    if (!pending.size) return;
    for (const x of r.swaps) {
        const t0 = pending.get(x.mint); if (t0 === undefined || !x.is_buy) continue;
        pending.delete(x.mint);
        if (!x.mayhem_mode && (!x.quote_mint || x.quote_mint === WSOL)) { buf.push({mint: x.mint, dev: x.user, creator: x.creator || null, t0}); S.late++; trackStart(x.mint, r.ts); trackSwaps({ts: r.ts, swaps: [x]}); }
    }
}
setInterval(() => { const old = Date.now() - 600000; for (const [m, t] of pending) if (t < old) pending.delete(m); }, 60000).unref();
if (ON) setInterval(async () => {
    if (!buf.length) return;
    const b = buf; buf = [];
    try {
        const res = await fetch(URL_, {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify(b), signal: AbortSignal.timeout(3000)});
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        S.sent += b.length;
    } catch (e) { S.errors++; if (S.errors <= 20 || S.errors % 100 === 0) logger.warn(`launches: не отправлено (${b.length}): ${e.message}`); }
}, FLUSH_MS).unref();
if (PATHS_ON) setInterval(async () => {
    const now = Date.now(), out = [];
    for (const [m, t] of track) if (now - t.at >= PATHS_AFTER_MS) { track.delete(m); if (t.pts.length) out.push({mint: m, t0: t.t0, ...buildPath(t)}); }
    if (!out.length) return;
    try {
        const res = await fetch(PATHS_URL, {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify(out), signal: AbortSignal.timeout(5000)});
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        SP.sent += out.length;
    } catch (e) { SP.errors++; if (SP.errors <= 20 || SP.errors % 100 === 0) logger.warn(`paths: не отправлено (${out.length}): ${e.message}`); }
}, 1000).unref();
function stats() { const o = {...S, pending: pending.size, paths: PATHS_ON ? {...SP, open: track.size} : undefined}; Object.assign(S, {creates: 0, sent: 0, noSwap: 0, other: 0, late: 0, errors: 0}); Object.assign(SP, {tracked: 0, sent: 0, errors: 0}); return o; }
module.exports = {onPumpBc, onSwaps, stats, ON};
