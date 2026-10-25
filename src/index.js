/**
 * sol-collector-hub: единая точка между Triton (Yellowstone gRPC) и ботами.
 *  - фандинг криейторов новых монет (Triton RPC; на 3-й сделке, первые 2 — покупки) → канал funding и pump_fun_funding;
 *  - метадата новых монет pump.fun (IPFS) → канал meta и pump_fun_bc_meta;
 *  - BNB Chain: Four.meme и Flap.sh по websocket (src/sources/bsc/);
 *  - один поток gRPC по программам площадок (pump.fun кривая, Raydium LaunchLab, Meteora DBC) + поток pump.fun AMM (миграции и пулы первые 60 мин);
 *  - разбор сделок и пулов, запись в MySQL (те же таблицы, что у коллекторов и трейдера copy-trade-solana);
 *  - раздача разобранного по TCP (src/server.js): клиент сам выбирает каналы.
 *
 *   node src/index.js          SOURCES=pump_bc,pump_amm,launchlab,meteora_dbc   DB_WRITE=1   HUB_PORT=7710
 */
require('dotenv').config({path: require('path').join(__dirname, '../.env'), quiet: true});
const mysql = require('mysql2/promise');
const {monitorEventLoopDelay} = require('perf_hooks');
const {txMeta, BulkWriter, streamPrograms, logger} = require('./common');
const {createServer} = require('./server');
const {createPumpBc} = require('./sources/pump-bc');
/** «скрытые» движения SOL (подарки / переводы WSOL, закрытие счёта на чужой) → сборщик переводов copy-trade-solana (src/hidden-sink.js) */
const hidden = require('./hidden-sink');
/** запуски монет → кластеры создателей в сборщике переводов (src/launch-sink.js) */
const launchSink = require('./launch-sink');
logger.level = process.env.LOG_LEVEL || 'info';

const SOURCES = (process.env.SOURCES || 'pump_bc,pump_amm,launchlab,meteora_dbc,blockhash,bsc,meta,funding').split(',').map(s => s.trim()).filter(Boolean);
const WRITE = process.env.DB_WRITE !== '0';
const LP = {launchlab: require('./sources/launchlab'), meteora_dbc: require('./sources/meteora-dbc')};

(async () => {
    const db = mysql.createPool({host: process.env.MYSQL_HOST, user: process.env.MYSQL_USERNAME, password: process.env.MYSQL_PASSWORD,
        database: process.env.MYSQL_DATABASE, connectionLimit: 6, charset: 'utf8mb4'});
    // таблицы MySQL при старте не создаются и не меняются (24.10): схема — sql/schema.sql, накатывается вручную

    const hub = createServer();
    /** метадата: по записи создания монеты из pump_bc (источник meta требует pump_bc) */
    const meta = SOURCES.includes('meta') ? require('./sources/pump-meta').createPumpMeta({db, write: WRITE, publish: hub.publish}) : null;
    /** фандинг: по записям pump_bc (создание и сделки) → канал funding и pump_fun_funding */
    const funding = SOURCES.includes('funding') ? require('./sources/pump-funding').createPumpFunding({db, write: WRITE, publish: hub.publish}) : null;
    const publish = meta || funding || launchSink.ON ? (ch, r) => {
        hub.publish(ch, r);
        if (ch === 'pump_bc' && launchSink.ON) { if (r.create) launchSink.onPumpBc(r); else launchSink.onSwaps(r); }
        if (ch !== 'pump_bc') return;
        if (meta && r.create) meta.onCreate(r);
        if (funding) funding.onRecord(r);
    } : hub.publish;

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
        if (hidden.ON) setImmediate(() => hidden.push(d));   // после раздачи ботам
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

    /** приём от сервисов на этом же сервере (25.10): POST /farm — снимок ферм операторов от сборщика переводов → канал farm. HUB_INGEST_PORT (7711), 0 — выкл. */
    const INGEST_PORT = +(process.env.HUB_INGEST_PORT ?? 7711);
    if (INGEST_PORT) require('http').createServer((req, res) => {
        if (req.method !== 'POST' || req.url !== '/farm') { res.writeHead(404); res.end(); return; }
        let b = ''; req.on('data', d => { b += d; if (b.length > 2e7) req.destroy(); });
        req.on('end', () => { try { const x = JSON.parse(b); if (!Array.isArray(x.prob) || !Array.isArray(x.ban)) throw new Error('нет prob / ban'); hub.publish('farm', {epoch: x.epoch, ver: x.ver, prob: x.prob, ban: x.ban, ts: Date.now()}); res.writeHead(200); res.end('{"ok":1}'); }
            catch (e) { res.writeHead(400); res.end(JSON.stringify({error: e.message})); } });
    }).listen(INGEST_PORT, process.env.HUB_INGEST_BIND || '127.0.0.1', () => logger.info(`приём: 127.0.0.1:${INGEST_PORT} (POST /farm)`));
    const amm = SOURCES.includes('pump_amm') ? await require('./sources/pump-amm-runner').startPumpAmm({db, write: WRITE, publish}) : null;
    const bh = SOURCES.includes('blockhash') ? require('./sources/blockhash').startBlockhash({publish}) : null;
    /** BNB Chain: Four.meme и Flap.sh (websocket BSC_WS_URL, не Triton) */
    const bsc = SOURCES.includes('bsc') ? {four_meme: require('./sources/bsc/four-meme').startFourMeme({db, write: WRITE, publish}),
        flap: require('./sources/bsc/flap').startFlap({db, write: WRITE, publish})} : null;

    /** метрики раз в минуту */
    const el = monitorEventLoopDelay({resolution: 1}); el.enable();
    let cpu0 = process.cpuUsage(), t0 = Date.now(), pub0 = 0, bytes0 = 0;
    setInterval(() => {
        const cpu = process.cpuUsage(cpu0), wall = Date.now() - t0; cpu0 = process.cpuUsage(); t0 = Date.now();
        const s = [...lag].sort((a, b) => a - b); lag.length = 0; const q = p => s.length ? s[Math.min(s.length - 1, Math.floor(p * s.length))] : null;
        logger.info({hub: {lagMs: {p50: q(.5), p90: q(.9), p99: q(.99), max: s.at(-1) ?? null, n: s.length}, cpuPct: +((cpu.user + cpu.system) / 1000 / wall * 100).toFixed(1),
            eventLoopP99Ms: +(el.percentile(99) / 1e6).toFixed(2), publishedMin: hub.stats.published - pub0, mbOutMin: +((hub.stats.bytes - bytes0) / 1048576).toFixed(1),
            mem: (() => { const m = process.memoryUsage(); return `rss ${(m.rss / 1048576).toFixed(0)} heap ${(m.heapUsed / 1048576).toFixed(0)} external ${(m.external / 1048576).toFixed(0)} arrayBuffers ${(m.arrayBuffers / 1048576).toFixed(0)} МБ`; })(),
            rings: Object.entries(hub.ringStats()).map(([k, v]) => `${k}:${v}`).join(' '),
            clients: [...hub.clients].map(c => `${c.name}:${[...c.subs].length}`), pump_bc: bc && {...bc.stats}, hidden: hidden.ON ? hidden.stats() : undefined, launches: launchSink.ON ? launchSink.stats() : undefined, blockhash: bh && {...bh.stats}, funding: funding && Object.entries(funding.stats()).filter(([, v]) => typeof v !== 'object').map(([k, v]) => `${k}=${v}`).join(' '),
            meta: meta && Object.entries(meta.stats).map(([k, v]) => `${k}=${v}`).join(' '),
            bsc: bsc && Object.entries(bsc).map(([n, x]) => `${n} ${Object.entries(x.stats).map(([k, v]) => `${k}=${v}`).join(' ')}`).join('; '),
            lp: Object.fromEntries(Object.entries(P).map(([n, p]) => [n, {...p.stats, written: p.swaps.written}]))}});
        el.reset(); pub0 = hub.stats.published; bytes0 = hub.stats.bytes;
    }, 60000);

    let exiting = false;
    const exit = async sig => {
        if (exiting) return; exiting = true;
        logger.warn(`выход (${sig}), дописываю буферы`);
        if (stream) stream.stop();
        await Promise.race([Promise.all([bc && bc.close(), ...Object.values(P).flatMap(p => [p.swaps.close(), p.pools.close(), p.completes && p.completes.close()]), amm && amm.stop(), funding && funding.close(),
            ...(bsc ? Object.values(bsc).map(x => x.stop()) : [])]),
            new Promise(r => setTimeout(r, 5000))]);
        await db.end().catch(() => {});
        process.exit(0);
    };
    process.on('SIGTERM', () => exit('SIGTERM'));
    process.on('SIGINT', () => exit('SIGINT'));
    logger.info(`хаб запущен: источники ${SOURCES.join(', ')}, запись в MySQL ${WRITE ? 'включена' : 'выключена'}`);
})().catch(e => { logger.error(e.stack || e.message); process.exit(1); });
