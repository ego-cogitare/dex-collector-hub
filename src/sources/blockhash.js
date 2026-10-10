/**
 * Источник blockhash: раз в BLOCKHASH_MS (1 с) getLatestBlockhash (commitment confirmed, наш Triton RPC) → канал blockhash {hash, slot, lastValidBlockHeight, ts}
 * при каждой смене хэша. Заменяет services/blockhash.js copy-trade-solana (раньше — ключи blockhash:latest:<hash>:<ts>:<slot> в Redis раз в 5 с).
 * Почему RPC, а не blocksMeta потока: поток — commitment processed, хэш блока на отброшенной ветке дал бы «blockhash not found».
 */
const {logger} = require('../common');

function startBlockhash({publish, everyMs = +(process.env.BLOCKHASH_MS || 1000), url = process.env.RPC}) {
    let last = null, errors = 0;
    const stats = {published: 0, errors: 0};
    const tick = async () => {
        try {
            const r = await fetch(url, {method: 'POST', headers: {'content-type': 'application/json'}, signal: AbortSignal.timeout(5000),
                body: JSON.stringify({jsonrpc: '2.0', id: 1, method: 'getLatestBlockhash', params: [{commitment: 'confirmed'}]})});
            const j = await r.json();
            if (j.error) throw new Error(JSON.stringify(j.error));
            const v = j.result.value, slot = j.result.context.slot;
            if (v.blockhash !== last) { last = v.blockhash; publish('blockhash', {hash: v.blockhash, slot, lastValidBlockHeight: v.lastValidBlockHeight, ts: Date.now()}); stats.published++; }
            errors = 0;
        } catch (e) { stats.errors++; if (++errors <= 3 || errors % 60 === 0) logger.error(`blockhash: ${e.message}`); }
    };
    tick(); const timer = setInterval(tick, everyMs);
    logger.info(`blockhash: раз в ${everyMs} мс, RPC ${url ? 'задан' : 'НЕ ЗАДАН'}`);
    return {stats, stop: () => clearInterval(timer)};
}

module.exports = {startBlockhash};
