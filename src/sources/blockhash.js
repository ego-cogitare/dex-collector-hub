/**
 * Источник blockhash → канал blockhash {hash, slot, lastValidBlockHeight, ts} при каждой смене хэша (боты подписывают им транзакции).
 * Основной путь — отдельная подписка Yellowstone gRPC blocksMeta с commitment confirmed (без вызовов RPC): blockhash каждого подтверждённого блока.
 *   Поток свопов не годится — он processed: хэш блока на отброшенной ветке дал бы «blockhash not found».
 * Запасной путь — RPC getLatestBlockhash (confirmed, наш Triton) раз в BLOCKHASH_RPC_MS (3 с), только пока подписка молчит дольше BLOCKHASH_SILENT_MS (3 с);
 *   как только блоки снова идут — опрос RPC прекращается. Подписка переподключается при ошибке/конце и при тишине дольше BLOCKHASH_STALL_MS (10 с).
 * Публикуется только хэш свежее последнего опубликованного (по слоту), чтобы два источника не перемешали порядок.
 * lastValidBlockHeight у blocksMeta — высота блока + 300, как у getLatestBlockhash (сверено 15.10).
 * Заменяет services/blockhash.js copy-trade-solana (ключи blockhash:latest:* в Redis раз в 5 с) и прежний опрос RPC раз в 1 с.
 */
const {logger} = require('../common');

const SILENT_MS = +(process.env.BLOCKHASH_SILENT_MS || 3000);
const RPC_MS = +(process.env.BLOCKHASH_RPC_MS || 3000);
const STALL_MS = +(process.env.BLOCKHASH_STALL_MS || 10000);
const MAX_AGE_BLOCKS = 300;

function startBlockhash({publish, url = process.env.RPC}) {
    const stats = {published: 0, grpc: 0, rpc: 0, rpcCalls: 0, fallbacks: 0, reconnects: 0, errors: 0, mode: 'grpc'};
    let lastHash = null, lastSlot = -1, lastGrpc = Date.now(), errors = 0;

    function emit(hash, slot, lastValidBlockHeight, src) {
        if (!hash || hash === lastHash || slot <= lastSlot) return;
        lastHash = hash; lastSlot = slot;
        publish('blockhash', {hash, slot, lastValidBlockHeight, ts: Date.now()});
        stats.published++; stats[src]++;
    }

    /** ---------- gRPC blocksMeta (confirmed) ---------- */
    const {default: Client, CommitmentLevel} = require('@triton-one/yellowstone-grpc');
    const request = {accounts: {}, slots: {}, transactions: {}, transactionsStatus: {}, blocks: {}, blocksMeta: {blockhash: {}}, entry: {}, accountsDataSlice: [],
        commitment: CommitmentLevel.CONFIRMED};
    let current = null, stopped = false, reconnecting = false;
    async function connect() {
        const client = new Client(process.env.WS_RPC, process.env.TRITON_ONE_TOKEN, {'grpc.keepalive_time_ms': 20000});
        await client.connect();
        const stream = await client.subscribe();
        current = stream;
        stream.on('data', d => {
            const m = d.blockMeta;
            if (!m) return;
            lastGrpc = Date.now();
            const h = m.blockHeight ? Number(m.blockHeight.blockHeight) : null;
            emit(m.blockhash, Number(m.slot), h === null ? null : h + MAX_AGE_BLOCKS, 'grpc');
        });
        stream.on('error', e => { if (stream === current) { logger.error('blockhash: подписка', e.message); reconnect('ошибка'); } });
        stream.on('end', () => { if (stream === current) reconnect('конец потока'); });
        await new Promise((resolve, reject) => stream.write(request, e => e ? reject(e) : resolve()));
        logger.info('blockhash: подписка blocksMeta (confirmed)');
    }
    async function reconnect(why) {
        if (reconnecting || stopped) return;
        reconnecting = true; stats.reconnects++;
        logger.warn(`blockhash: переподключение подписки (${why})`);
        const old = current; current = null;
        try { old && old.destroy && old.destroy(); } catch (e) {}
        for (let i = 0; !stopped; i++) {
            await new Promise(r => setTimeout(r, Math.min(30000, 1000 * 2 ** i)));
            try { await connect(); lastGrpc = Date.now(); break; } catch (e) { stats.errors++; logger.error('blockhash: подключение', e.message); }
        }
        reconnecting = false;
    }
    connect().catch(e => { stats.errors++; logger.error('blockhash: подключение', e.message); reconnect('старт'); });

    /** ---------- запасной RPC, только пока подписка молчит ---------- */
    async function rpcTick() {
        try {
            stats.rpcCalls++;
            const r = await fetch(url, {method: 'POST', headers: {'content-type': 'application/json'}, signal: AbortSignal.timeout(5000),
                body: JSON.stringify({jsonrpc: '2.0', id: 1, method: 'getLatestBlockhash', params: [{commitment: 'confirmed'}]})});
            const j = await r.json();
            if (j.error) throw new Error(JSON.stringify(j.error));
            emit(j.result.value.blockhash, j.result.context.slot, j.result.value.lastValidBlockHeight, 'rpc');
            errors = 0;
        } catch (e) { stats.errors++; if (++errors <= 3 || errors % 20 === 0) logger.error(`blockhash rpc: ${e.message}`); }
    }
    let lastRpc = 0;
    const watch = setInterval(() => {
        const now = Date.now(), silent = now - lastGrpc;
        if (silent > SILENT_MS) {
            if (stats.mode !== 'rpc') { stats.mode = 'rpc'; stats.fallbacks++; logger.warn(`blockhash: blocksMeta молчит ${silent} мс — RPC раз в ${RPC_MS} мс`); }
            if (now - lastRpc >= RPC_MS) { lastRpc = now; rpcTick(); }
            if (silent > STALL_MS) reconnect(`тишина ${Math.round(silent / 1000)} с`);
        } else if (stats.mode !== 'grpc') { stats.mode = 'grpc'; logger.info('blockhash: blocksMeta снова идёт — RPC выключен'); }
    }, 250);

    logger.info(`blockhash: gRPC blocksMeta (confirmed); запасной RPC ${url ? 'задан' : 'НЕ ЗАДАН'} раз в ${RPC_MS} мс при тишине > ${SILENT_MS} мс`);
    return {stats, stop: () => { stopped = true; clearInterval(watch); try { current && current.destroy && current.destroy(); } catch (e) {} }};
}

module.exports = {startBlockhash};
