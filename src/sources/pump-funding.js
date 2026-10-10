/**
 * Фандинг кошельков новых монет pump.fun (трассировщик Helius, src/funding-tracer.js) → pump_fun_funding.
 * Перенесено из copy-trade-solana sandbox/trader.js (FETCH_FUNDING_HISTORY=1), правила те же:
 *  - монеты, созданные при работающем хабе, с хотя бы одной сделкой в SOL и не mayhem; окно 180 с от создания (MINT_DEL_TIMEOUT трейдера);
 *  - как только у монеты больше FETCH_FUNDING_MIN_SWAPS сделок (15) — трассируем первых FETCH_FUNDING_WALLETS_N покупателей (1 — только криейтор,
 *    своп 0 — traceCreator, остальные — traceBuyer); t0 — время транзакции создания;
 *  - глубина FETCH_FUNDING_DEPTH хопов (по умолчанию 3, у правила A было 1).
 * Кэш хопов — FUNDING_CACHE_FILE (data/funding_cache.jsonl), копия результатов — FUNDING_OUT_FILE (data/funding_results.jsonl).
 * Боты в решениях фандинг не используют — канала нет, только запись.
 */
const fs = require('fs');
const path = require('path');
const {logger} = require('../common');
const FundingTracer = require('../funding-tracer');
const {parseCreate} = require('./pump-meta');

const WSOL = 'So11111111111111111111111111111111111111112';
const WINDOW_MS = 180000;
const MIN_SWAPS = parseInt(process.env.FETCH_FUNDING_MIN_SWAPS || '15');
const WALLETS_N = Math.max(1, parseInt(process.env.FETCH_FUNDING_WALLETS_N) || 1);
const solOk = s => !s.mayhem_mode && (s.quote_mint === WSOL || !s.quote_mint);

function createPumpFunding({db, write}) {
    if (!process.env.HELIUS_API_KEY) throw new Error('funding: нужен HELIUS_API_KEY (через запятую)');
    const dir = path.join(__dirname, '../../data'); fs.mkdirSync(dir, {recursive: true});
    let buf = [];
    const flush = () => {
        if (!buf.length || !db || !write) { buf = []; return Promise.resolve(); }
        const rows = buf; buf = [];
        return db.query('INSERT INTO pump_fun_funding(mint, wallet, t0, traced_at, data) VALUES ?', [rows]).catch(e => logger.error('pump_fun_funding', e.message));
    };
    const timer = setInterval(flush, 5000);
    const stats = {mints: 0, traced: 0, results: 0};
    const tracer = new FundingTracer({
        apiKeys: process.env.HELIUS_API_KEY.split(','),
        maxConcurrentTraces: 8,
        maxDepth: parseInt(process.env.FETCH_FUNDING_DEPTH || '3'),
        cacheFile: process.env.FUNDING_CACHE_FILE || path.join(dir, 'funding_cache.jsonl'),
        outFile: process.env.FUNDING_OUT_FILE || path.join(dir, 'funding_results.jsonl'),
        onResult: rec => {
            stats.results++;
            buf.push([rec.mint || null, rec.wallet || null, rec.t0 ?? null, rec.tracedAt ?? null, JSON.stringify(rec)]);
            if (buf.length >= 50) flush();
        },
    });

    /** mint → {t0, swaps: [{user, buy}], next, buys} */
    const mints = new Map();

    function onRecord(rec) {
        if (rec.creatorChange) return;
        const ok = rec.swaps.filter(solOk);
        if (rec.create && rec.createData && ok.length) {
            let mint; try { mint = parseCreate(rec.createData).mint; } catch (e) { mint = ok[0].mint; }
            if (!mints.has(mint)) {
                mints.set(mint, {t0: rec.ts, swaps: [], next: 0, buys: 0});
                stats.mints++;
                setTimeout(() => { mints.delete(mint); tracer.forgetMint(mint); }, WINDOW_MS + 5000);
            }
        }
        const touched = new Set();
        for (const s of ok) { const m = mints.get(s.mint); if (!m) continue; m.swaps.push({user: s.user, buy: !!s.is_buy && BigInt(s.total) > 0n}); touched.add(s.mint); }   // buy — как amountSol > 0 у трейдера
        for (const mint of touched) {
            const m = mints.get(mint);
            if (m.swaps.length <= MIN_SWAPS) continue;
            for (; m.next < m.swaps.length && m.buys < WALLETS_N; m.next++) {
                const s = m.swaps[m.next];
                if (!s.buy) continue;
                m.buys++; stats.traced++;
                (m.next === 0 ? tracer.traceCreator(mint, m.t0, s.user) : tracer.traceBuyer(mint, m.t0, s.user)).catch(e => logger.error('funding', e.message));
            }
        }
    }

    return {
        onRecord: rec => { try { onRecord(rec); } catch (e) { logger.error('funding', e.message); } },
        stats: () => ({...stats, ...tracer.stats()}),
        close: async () => { clearInterval(timer); await flush(); },
    };
}

module.exports = {createPumpFunding};
