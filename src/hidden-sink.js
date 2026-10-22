/**
 * «Скрытые» движения SOL в потоках площадок (src/hidden-moves.js: подарок WSOL-счёта, перевод WSOL между владельцами, закрытие счёта на чужой кошелёк)
 * → сборщик переводов copy-trade-solana (tools/sol-transfers.js на этом же сервере): POST EDGES_URL (http://10.156.0.18:7730/edges) пачками раз в EDGES_FLUSH_MS (1000).
 * Разбор — после раздачи сделок ботам (задержку ботам не добавляет); HIDDEN_EDGES=0 — выключено. Сводка — stats() в минутной строке хаба.
 */
'use strict';
const {scan, quick, fromGrpc} = require('./hidden-moves');
const {logger} = require('./common');
const ON = process.env.HIDDEN_EDGES !== '0';
const URL_ = process.env.EDGES_URL || 'http://10.156.0.18:7730/edges';
const FLUSH_MS = parseInt(process.env.EDGES_FLUSH_MS || '1000');
let buf = [];
const seen = new Map();
const S = {tx: 0, full: 0, edges: 0, sent: 0, errors: 0, us: 0, kinds: {}};
function push(d) {
    if (!ON || !d || !d.transaction) return;
    const t0 = process.hrtime.bigint();
    try {
        S.tx++;
        if (!quick(d)) return;   // предфильтр по сырым байтам: обычные свопы (≈ 99.5%) — без полного разбора
        const n = fromGrpc(d);
        if (seen.has(n.sig)) return;
        seen.set(n.sig, Date.now()); S.full++;
        const slot = Number(d.transaction.slot), t = Date.now();
        for (const e of scan(n)) { buf.push({...e, sig: n.sig, slot, t}); S.edges++; S.kinds[e.kind] = (S.kinds[e.kind] || 0) + 1; }
    } catch (e) { S.errors++; if (S.errors <= 5) logger.error(`hidden: ${e.message}`); }
    S.us += Number(process.hrtime.bigint() - t0) / 1000;
}
if (ON) {
    setInterval(async () => {
        if (!buf.length) return;
        const b = buf; buf = [];
        try {
            const r = await fetch(URL_, {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify(b), signal: AbortSignal.timeout(5000)});
            if (!r.ok) throw new Error(`HTTP ${r.status}`);
            S.sent += b.length;
        } catch (e) { S.errors++; if (S.errors <= 20 || S.errors % 100 === 0) logger.warn(`hidden: связи не отправлены (${b.length}): ${e.message}`); }
    }, FLUSH_MS).unref();
    setInterval(() => { const old = Date.now() - 600000; for (const [k, v] of seen) { if (v < old) seen.delete(k); else break; } }, 60000).unref();
}
/** сводка за период и сброс */
function stats() { const o = {...S, usPerTx: S.tx ? +(S.us / S.tx).toFixed(1) : 0}; delete o.us; Object.assign(S, {tx: 0, full: 0, edges: 0, sent: 0, errors: 0, us: 0, kinds: {}}); return o; }
module.exports = {push, stats, ON};
