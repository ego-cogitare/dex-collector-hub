/**
 * sol-collector-hub: единая точка между Triton (Yellowstone gRPC) и ботами.
 *  - один поток gRPC по программам площадок (pump.fun кривая, Raydium LaunchLab, Meteora DBC) + поток pump.fun AMM (миграции и пулы первые 60 мин);
 *  - разбор сделок и пулов, запись в MySQL (те же таблицы, что у коллекторов и трейдера copy-trade-solana);
 *  - раздача разобранного по TCP (src/server.js): клиент сам выбирает каналы.
 *
 *   node src/index.js          SOURCES=pump_bc,pump_amm,launchlab,meteora_dbc   DB_WRITE=1   HUB_PORT=7710
 */
require('dotenv').config({path: require('path').join(__dirname, '../.env'), quiet: true});
const fs = require('fs');
const path = require('path');
const mysql = require('mysql2/promise');
const {monitorEventLoopDelay} = require('perf_hooks');
const {txMeta, BulkWriter, streamPrograms, logger} = require('./common');
const {createServer} = require('./server');
const {createPumpBc} = require('./sources/pump-bc');
logger.level = process.env.LOG_LEVEL || 'info';

const SOURCES = (process.env.SOURCES || 'pump_bc,pump_amm,launchlab,meteora_dbc').split(',').map(s => s.trim()).filter(Boolean);
const WRITE = process.env.DB_WRITE !== '0';
const LP = {launchlab: require('./sources/launchlab'), meteora_dbc: require('./sources/meteora-dbc')};

(async () => {
    const db = mysql.createPool({host: process.env.MYSQL_HOST, user: process.env.MYSQL_USERNAME, password: process.env.MYSQL_PASSWORD,
        database: process.env.MYSQL_DATABASE, connectionLimit: 6, charset: 'utf8mb4'});
    if (WRITE) for (const stmt of fs.readFileSync(path.join(__dirname, '../schema.sql'), 'utf8').replace(/^--.*$/gm, '').split(';').map(s => s.trim()).filter(Boolean)) await db.query(stmt);

    const hub = createServer();
    const publish = hub.publish;

    /** площадки на общем потоке */
    const lag = [];   // createdAt Triton → приход, мс (за минуту)
    const bc = SOURCES.includes('pump_bc') ? createPumpBc({db, write: WRITE, publish}) : null;
    const P = {};
    for (const name of SOURCES.filter(n => LP[n])) {
        const m = LP[name];
        const mk = (table, cols, opt) => { const w = new BulkWriter(db, table, cols, opt); if (!WRITE) w.push = () => {}; return w; };
        P[name] = {m, stats: {tx: 0, swaps: 0, pools: 0, completes: 0, errors: 0},
            swaps: mk(`lp_${name}_swaps`, m.SWAP_COLUMNS, {skipDuplicates: true}), pools: mk(`lp_${name}_pools`, m.POOL_COLUMNS, {size: 50, ignore: true}),
            completes: m.COMPLETE_COLUMNS ? mk(`lp_${name}_completes`, m.COMPLETE_COLUMNS, {size: 20, skipDuplicates: true}) : null};
    }
    const seen = new Map(); setInterval(() => { const t = Date.now() - 600000; for (const [k, v] of seen) if (v < t) seen.delete(k); }, 60000);
    const filters = {};
    if (bc) filters.pump_bc = {accountRequired: [bc.PROGRAM]};
    for (const name of Object.keys(P)) filters[name] = P[name].m.PROGRAM;
    const stream = Object.keys(filters).length ? await streamPrograms(filters, (d, f) => {
        const now = Date.now(); if (d.createdAt) lag.push(now - new Date(d.createdAt).getTime());
        if (bc && f.includes('pump_bc')) { try { bc.onTx(d); } catch (e) { logger.error('pump_bc', e.message); } }
        const lps = Object.keys(P).filter(n => f.includes(n)); if (!lps.length) return;
        const meta = txMeta(d); if (meta.failed) return;
        if (seen.has(meta.signature)) return; seen.set(meta.signature, now);
        for (const name of lps) {
            const p = P[name]; p.stats.tx++;
            try {
                const r = p.m.decode(d.transaction.transaction, meta);
                for (const s of r.swaps) { p.swaps.push(s); publish(name, s); }
                for (const x of r.pools) { p.pools.push(x); publish(`${name}_pool`, x); }
                for (const x of r.completes || []) { if (p.completes) p.completes.push(x); publish(`${name}_complete`, x); }
                p.stats.swaps += r.swaps.length; p.stats.pools += r.pools.length; p.stats.completes += (r.completes || []).length;
            } catch (e) { p.stats.errors++; if (p.stats.errors <= 5) logger.error(`${name} ${meta.signature}: ${e.message}`); }
        }
    }, {stallMs: 20000}) : null;

    const amm = SOURCES.includes('pump_amm') ? await require('./sources/pump-amm-runner').startPumpAmm({db, write: WRITE, publish}) : null;

    /** метрики раз в минуту */
    const el = monitorEventLoopDelay({resolution: 1}); el.enable();
    let cpu0 = process.cpuUsage(), t0 = Date.now(), pub0 = 0, bytes0 = 0;
    setInterval(() => {
        const cpu = process.cpuUsage(cpu0), wall = Date.now() - t0; cpu0 = process.cpuUsage(); t0 = Date.now();
        const s = [...lag].sort((a, b) => a - b); lag.length = 0; const q = p => s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))] : null;
        logger.info({hub: {lagMs: {p50: q(.5), p90: q(.9), p99: q(.99), max: s.at(-1) ?? null, n: s.length}, cpuPct: +((cpu.user + cpu.system) / 1000 / wall * 100).toFixed(1),
            eventLoopP99Ms: +(el.percentile(99) / 1e6).toFixed(2), publishedMin: hub.stats.published - pub0, mbOutMin: +((hub.stats.bytes - bytes0) / 1048576).toFixed(1),
            clients: [...hub.clients].map(c => `${c.name}:${[...c.subs].length}`), pump_bc: bc && {...bc.stats},
            lp: Object.fromEntries(Object.entries(P).map(([n, p]) => [n, {...p.stats, written: p.swaps.written}]))}});
        el.reset(); pub0 = hub.stats.published; bytes0 = hub.stats.bytes;
    }, 60000);

    let exiting = false;
    const exit = async sig => {
        if (exiting) return; exiting = true;
        logger.warn(`выход (${sig}), дописываю буферы`);
        if (stream) stream.stop();
        await Promise.race([Promise.all([bc && bc.close(), ...Object.values(P).flatMap(p => [p.swaps.close(), p.pools.close(), p.completes && p.completes.close()]), amm && amm.stop()]),
            new Promise(r => setTimeout(r, 5000))]);
        await db.end().catch(() => {});
        process.exit(0);
    };
    process.on('SIGTERM', () => exit('SIGTERM'));
    process.on('SIGINT', () => exit('SIGINT'));
    logger.info(`хаб запущен: источники ${SOURCES.join(', ')}, запись в MySQL ${WRITE ? 'включена' : 'выключена'}`);
})().catch(e => { logger.error(e.stack || e.message); process.exit(1); });
