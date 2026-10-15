/**
 * Фандинг криейторов новых монет pump.fun (src/funding-tracer.js) → канал funding и pump_fun_funding.
 *
 * Когда трассируем (15.10, под фильтр LOW по фондированию криейтора):
 *  - монеты, созданные при работающем хабе, со сделками в SOL и не mayhem; окно 180 с от создания;
 *  - на FETCH_FUNDING_AT_SWAP-й сделке монеты (3), если первые FETCH_FUNDING_FIRST_BUYS сделок (2) — покупки.
 *    На истории 01–12.10: отсекает 43% монет, теряет 0.09% сигналов LOW 0.03; от 3-й сделки до сигнала LOW — медиана 1.9 с, < 0.5 с у 5%;
 *  - трассируем только криейтора (своп 0, обычно покупка дева в транзакции создания); t0 — время транзакции создания;
 *  - глубина FETCH_FUNDING_DEPTH хопов (2), без recentIns (FETCH_FUNDING_RECENT=1 — включить; это самая дорогая часть трассы).
 * RPC: FUNDING_RPC_URL, иначе Triton (https://api.rpcpool.com/<TRITON_ONE_TOKEN>), иначе Helius (HELIUS_API_KEY через запятую).
 * Темп: FUNDING_RPS вызовов/с на адрес (Triton — 40; у бесплатного Helius было 6).
 * Результат: канал funding (боты решают по нему до входа) и строка в pump_fun_funding.
 * Кэш хопов — FUNDING_CACHE_FILE (data/funding_cache.jsonl), копия результатов — FUNDING_OUT_FILE (data/funding_results.jsonl).
 */
const fs = require('fs');
const path = require('path');
const {logger} = require('../common');
const FundingTracer = require('../funding-tracer');
const {parseCreate} = require('./pump-meta');

const WSOL = 'So11111111111111111111111111111111111111112';
const WINDOW_MS = 180000;
const AT_SWAP = Math.max(1, parseInt(process.env.FETCH_FUNDING_AT_SWAP || '3'));
const FIRST_BUYS = Math.max(0, parseInt(process.env.FETCH_FUNDING_FIRST_BUYS ?? '2'));
const solOk = s => !s.mayhem_mode && (s.quote_mint === WSOL || !s.quote_mint);

function rpcConfig() {
    if (process.env.FUNDING_RPC_URL) return {rpcUrl: process.env.FUNDING_RPC_URL, rps: 40, name: 'FUNDING_RPC_URL'};
    if (process.env.TRITON_ONE_TOKEN) return {rpcUrl: `https://api.rpcpool.com/${process.env.TRITON_ONE_TOKEN}`, rps: 40, name: 'Triton'};
    if (process.env.HELIUS_API_KEY) return {apiKeys: process.env.HELIUS_API_KEY.split(','), rps: 6, name: 'Helius'};
    throw new Error('funding: нужен FUNDING_RPC_URL, TRITON_ONE_TOKEN или HELIUS_API_KEY');
}

function createPumpFunding({db, write, publish}) {
    const rpc = rpcConfig();
    const dir = path.join(__dirname, '../../data'); fs.mkdirSync(dir, {recursive: true});
    let buf = [];
    const flush = () => {
        if (!buf.length || !db || !write) { buf = []; return Promise.resolve(); }
        const rows = buf; buf = [];
        return db.query('INSERT INTO pump_fun_funding(mint, wallet, t0, traced_at, data) VALUES ?', [rows]).catch(e => logger.error('pump_fun_funding', e.message));
    };
    const timer = setInterval(flush, 5000);
    const stats = {mints: 0, skippedFirstBuys: 0, traced: 0, results: 0, errors: 0, traceMs: []};
    const tracer = new FundingTracer({
        ...(rpc.rpcUrl ? {rpcUrl: rpc.rpcUrl} : {apiKeys: rpc.apiKeys}),
        perKeyRps: parseFloat(process.env.FUNDING_RPS || rpc.rps),
        maxConcurrentTraces: parseInt(process.env.FUNDING_CONCURRENCY || '16'),
        maxDepth: parseInt(process.env.FETCH_FUNDING_DEPTH || '2'),
        recentInsMode: process.env.FETCH_FUNDING_RECENT === '1' ? 'creator-only' : 'none',
        cacheFile: process.env.FUNDING_CACHE_FILE || path.join(dir, 'funding_cache.jsonl'),
        outFile: process.env.FUNDING_OUT_FILE || path.join(dir, 'funding_results.jsonl'),
        onResult: rec => {
            stats.results++;
            if (rec.traceMs !== undefined) stats.traceMs.push(rec.traceMs);
            if (publish) publish('funding', rec);
            buf.push([rec.mint || null, rec.wallet || null, rec.t0 ?? null, rec.tracedAt ?? null, JSON.stringify(rec)]);
            if (buf.length >= 50) flush();
        },
    });
    logger.info(`funding: RPC ${rpc.name}, ${tracer.cfg.perKeyRps} вызовов/с, глубина ${tracer.cfg.maxDepth}, recentIns ${tracer.cfg.recentInsMode}, трасса на ${AT_SWAP}-й сделке, первые ${FIRST_BUYS} — покупки`);

    /** mint → {t0, swaps: [{user, buy}], done} */
    const mints = new Map();

    function onRecord(rec) {
        if (rec.creatorChange) return;
        const ok = rec.swaps.filter(solOk);
        if (rec.create && rec.createData && ok.length) {
            let mint; try { mint = parseCreate(rec.createData).mint; } catch (e) { mint = ok[0].mint; }
            if (!mints.has(mint)) {
                mints.set(mint, {t0: rec.ts, swaps: [], done: false});
                stats.mints++;
                setTimeout(() => { mints.delete(mint); tracer.forgetMint(mint); }, WINDOW_MS + 5000);
            }
        }
        const touched = new Set();
        for (const s of ok) { const m = mints.get(s.mint); if (!m || m.done) continue; m.swaps.push({user: s.user, buy: !!s.is_buy && BigInt(s.total) > 0n}); touched.add(s.mint); }   // buy — как amountSol > 0 у трейдера
        for (const mint of touched) {
            const m = mints.get(mint);
            if (m.done || m.swaps.length < AT_SWAP) continue;
            m.done = true;
            if (!m.swaps.slice(0, FIRST_BUYS).every(s => s.buy)) { stats.skippedFirstBuys++; continue; }
            const creator = m.swaps[0];
            if (!creator.buy) { stats.skippedFirstBuys++; continue; }
            stats.traced++;
            tracer.traceCreator(mint, m.t0, creator.user).catch(e => { stats.errors++; logger.error('funding', e.message); });
        }
    }

    return {
        onRecord: rec => { try { onRecord(rec); } catch (e) { logger.error('funding', e.message); } },
        stats: () => {
            const t = stats.traceMs.sort((a, b) => a - b), q = p => t.length ? t[Math.min(t.length - 1, Math.floor(p * t.length))] : null;
            const {traceMs, ...counters} = stats;
            const out = {...counters, traceN: t.length, traceP50: q(0.5), traceP90: q(0.9), traceP99: q(0.99), ...tracer.stats()};   // сводка хаба печатает только плоские поля
            stats.traceMs = [];   // окно статистики — между вызовами stats()
            return out;
        },
        close: async () => { clearInterval(timer); await flush(); },
    };
}

module.exports = {createPumpFunding};
