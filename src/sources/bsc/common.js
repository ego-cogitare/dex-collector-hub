/**
 * BSC: подписка на логи контракта по websocket (eth_subscribe logs), разбор ABI, запись INSERT IGNORE.
 * Перенесено из node/bsc/lib/common.js (collector-amm-01) без изменения строк в MySQL.
 * Пропущенное за время разрыва websocket не дозапрашивается (как и раньше).
 */
const WebSocket = require('ws');
const log4js = require('log4js');

const BULK_SIZE = 100;
const FLUSH_INTERVAL = 1000;
const PING_INTERVAL = 15_000;
const RECONNECT_DELAY = 1000;
const RPC_TIMEOUT = 10_000;

const getLogger = name => log4js.getLogger(`[bsc:${name}]`);

/** JSON-RPC по HTTP (BSC_RPC_URL), отдельно от websocket подписки */
async function rpcCall(method, params) {
    const res = await fetch(process.env.BSC_RPC_URL, {
        method: 'POST',
        headers: {'content-type': 'application/json'},
        body: JSON.stringify({jsonrpc: '2.0', id: 1, method, params}),
        signal: AbortSignal.timeout(RPC_TIMEOUT),
    });
    const msg = await res.json();
    if (msg.error) throw new Error(msg.error.message);
    return msg.result;
}

/** ABI: `hex` — data лога без 0x */
const word = (hex, i) => BigInt('0x' + hex.slice(i * 64, i * 64 + 64));
const address = (hex, i) => '0x' + hex.slice(i * 64 + 24, i * 64 + 64);
const string = (hex, offset, maxLength = 255) => {
    const start = offset * 2;
    const length = Number(BigInt('0x' + hex.slice(start, start + 64)));
    return Buffer.from(hex.slice(start + 64, start + 64 + length * 2), 'hex').toString('utf8').slice(0, maxLength);
};

/** uint256 с 18 знаками → точная десятичная строка */
const units = value => {
    const s = value.toString().padStart(19, '0');
    return `${s.slice(0, -18)}.${s.slice(-18)}`;
};

/** Мета лога — всегда 5 последних значений строки */
const META_COLUMNS = ['tx_hash', 'log_index', 'block', 'block_timestamp', 'received_at'];
const meta = (log, receivedAt) => [
    log.transactionHash,
    parseInt(log.logIndex, 16),
    parseInt(log.blockNumber, 16),
    log.blockTimestamp ? parseInt(log.blockTimestamp, 16) : null,
    receivedAt,
];

/** Буферизованный INSERT IGNORE в `table`; write=false — без записи */
function createWriter(db, write, table, columns, logger) {
    const sql = `INSERT IGNORE INTO ${table}(${[...columns, ...META_COLUMNS].join(', ')}) VALUES ?`;
    let rows = [];
    const flush = () => {
        const out = [];
        while (rows.length) {
            const data = rows.slice(0, BULK_SIZE);
            rows = rows.slice(BULK_SIZE);
            out.push(db.query(sql, [data]).catch(e => logger.error(`${table}: ${e.message}`)));
        }
        return Promise.all(out);
    };
    const timer = setInterval(flush, FLUSH_INTERVAL);
    return {
        push: row => { if (write) rows.push(row); },
        /** Лог отменён реорганизацией: убрать строку из буфера и из таблицы */
        remove: (txHash, logIndex) => {
            if (!write) return;
            rows = rows.filter(row => row[row.length - 5] !== txHash || row[row.length - 4] !== logIndex);
            db.query(`DELETE FROM ${table} WHERE tx_hash = ? AND log_index = ?`, [txHash, logIndex])
                .catch(e => logger.error(`${table}: ${e.message}`));
        },
        close: () => { clearInterval(timer); return flush(); },
    };
}

/** Подписка на логи контракта, переподключение при обрыве. stats.logs — счётчик логов */
function subscribeLogs({address, topics, onLog, logger, stats}) {
    let ws, rpcId = 0, pending = {}, stopped = false;

    const rpc = (method, params) => new Promise((resolve, reject) => {
        const id = ++rpcId;
        const timer = setTimeout(() => { delete pending[id]; reject(new Error(`${method} timeout`)); }, RPC_TIMEOUT);
        pending[id] = {resolve, reject, timer};
        ws.send(JSON.stringify({jsonrpc: '2.0', id, method, params}));
    });

    const start = () => {
        let alive = true;
        ws = new WebSocket(process.env.BSC_WS_URL);
        ws.on('open', () => {
            rpc('eth_subscribe', ['logs', {address, topics: [topics]}])
                .then(id => logger.info(`подписка ${id}`))
                .catch(e => { logger.error(`подписка: ${e.message}`); ws.terminate(); });
        });
        ws.on('message', raw => {
            const msg = JSON.parse(raw);
            if (msg.method === 'eth_subscription') {
                const log = msg.params && msg.params.result;
                if (!log || !log.topics) return logger.warn(`неожиданное уведомление: ${String(raw).slice(0, 300)}`);
                stats.logs++;
                try { onLog(log, Date.now()); } catch (e) { stats.errors++; logger.error(`${log.transactionHash}:${log.logIndex} ${e.message}`); }
                return;
            }
            const req = pending[msg.id];
            if (!req) return;
            clearTimeout(req.timer);
            delete pending[msg.id];
            msg.error ? req.reject(new Error(msg.error.message)) : req.resolve(msg.result);
        });
        ws.on('pong', () => { alive = true; });
        /** мёртвое соединение — terminate, переподключает обработчик close */
        const ping = setInterval(() => {
            if (!alive) { logger.warn('нет pong — переподключение'); return ws.terminate(); }
            alive = false; ws.ping();
        }, PING_INTERVAL);
        ws.on('error', e => logger.error(e.message));
        ws.on('close', () => {
            clearInterval(ping);
            for (const id in pending) { clearTimeout(pending[id].timer); pending[id].reject(new Error('connection closed')); }
            pending = {};
            if (stopped) return;
            stats.reconnects++;
            logger.warn('websocket закрыт — переподключение');
            setTimeout(start, RECONNECT_DELAY);
        });
    };
    start();
    return {rpc, stop: () => { stopped = true; if (ws) ws.terminate(); }};
}

module.exports = {getLogger, rpcCall, word, address, string, units, meta, createWriter, subscribeLogs};
