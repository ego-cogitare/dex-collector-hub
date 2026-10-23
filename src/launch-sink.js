/**
 * Запуски монет pump.fun (SOL-кривая, не mayhem) → кластеры создателей в сборщике переводов copy-trade-solana (tools/creator-clusters.js):
 * POST LAUNCHES_URL (http://10.156.0.18:7730/launches) пачками раз в LAUNCHES_FLUSH_MS (300). Дев — первый покупатель в транзакции создания (как в симе).
 * LAUNCHES=0 — выключено. Боты спрашивают /launch?mint= (prior = 0 — новый кластер).
 */
'use strict';
const {logger} = require('./common');
const ON = process.env.LAUNCHES !== '0';
const URL_ = process.env.LAUNCHES_URL || 'http://10.156.0.18:7730/launches';
const FLUSH_MS = parseInt(process.env.LAUNCHES_FLUSH_MS || '300');
const WSOL = 'So11111111111111111111111111111111111111112';
let buf = [];
const S = {creates: 0, sent: 0, noSwap: 0, other: 0, late: 0, errors: 0};
const pending = new Map();   // создание без покупки в той же транзакции: монета → время создания (дев — первый покупатель позже)
function onPumpBc(r) {
    if (!ON || !r || !r.create) return;
    S.creates++;
    const sw = r.swaps || [];
    const s = sw.find(x => x.mint && !x.mayhem_mode && (!x.quote_mint || x.quote_mint === WSOL));
    if (s && s.user) { buf.push({mint: s.mint, dev: s.user, t0: r.ts}); return; }
    if (!sw.length) { S.noSwap++; const m = r.tokens && Object.keys(r.tokens)[0]; if (m) pending.set(m, r.ts); return; }
    S.other++;   // mayhem / не SOL — бот их не торгует
}
/** первая покупка монеты, созданной без покупки дева: дев — этот покупатель (как в симе: первый своп монеты) */
function onSwaps(r) {
    if (!ON || !pending.size || !r || !r.swaps) return;
    for (const x of r.swaps) {
        const t0 = pending.get(x.mint); if (t0 === undefined || !x.is_buy) continue;
        pending.delete(x.mint);
        if (!x.mayhem_mode && (!x.quote_mint || x.quote_mint === WSOL)) { buf.push({mint: x.mint, dev: x.user, t0}); S.late++; }
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
function stats() { const o = {...S, pending: pending.size}; Object.assign(S, {creates: 0, sent: 0, noSwap: 0, other: 0, late: 0, errors: 0}); return o; }
module.exports = {onPumpBc, onSwaps, stats, ON};
