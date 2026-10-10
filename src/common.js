/**
 * Общее для коллекторов площадок запуска (collectors/): поток Yellowstone gRPC по программам площадок с переподключением,
 * разбор транзакции (все ключи, инструкции вместе с внутренними, события Anchor emit_cpi), чтение Borsh и пакетная запись в MySQL.
 *
 * Сделки и пулы пишутся в MySQL (таблицы lp_<площадка>_swaps / lp_<площадка>_pools, collectors/schema.sql),
 * оттуда collectors/export.js переносит их в parquet (swaps/<площадка>/…) и удаляет из MySQL.
 * Суммы и резервы — сырые целые из событий (DECIMAL), без деления на разряды: так ничего не теряется.
 */
const bs58 = require('bs58').default;
const logger = require('log4js').getLogger('[hub]');

/** Цена и лимит CU из инструкций ComputeBudget (как getComputeUnitValues в bot/utils.js; своя копия — коллекторам не нужен весь бот) */
const COMPUTE_BUDGET = 'ComputeBudget111111111111111111111111111111';
function getComputeUnitValues(tx) {
    let unitPrice = null, unitLimit = null;
    for (const ix of tx.transaction.message.instructions) {
        if (bs58.encode(tx.transaction.message.accountKeys[ix.programIdIndex]) !== COMPUTE_BUDGET) continue;
        const data = Buffer.from(ix.data);
        if (data.length === 9 && data[0] === 3) unitPrice = data.readBigUInt64LE(1);   // SetComputeUnitPrice
        if (data[0] === 2) unitLimit = data.readUInt32LE(1);                          // SetComputeUnitLimit
    }
    return {computeUnitPrice: Number(unitPrice), computeUnitLimit: unitLimit};
}

/** Тег инструкции-события Anchor (emit_cpi): sha256('anchor:event')[0..8] */
const EVENT_IX_TAG = Buffer.from('e445a52e51cb9a1d', 'hex');

/** Все ключи транзакции gRPC: статические + загруженные из lookup-таблиц (запись, потом чтение) */
function allKeys(tx) {
    const keys = tx.transaction.message.accountKeys.map(k => bs58.encode(k));
    for (const k of tx.meta?.loadedWritableAddresses || []) keys.push(bs58.encode(k));
    for (const k of tx.meta?.loadedReadonlyAddresses || []) keys.push(bs58.encode(k));
    return keys;
}

/** Все инструкции транзакции по порядку исполнения: внешняя, затем её внутренние. {program, accounts: [base58], data: Buffer, outer, inner} */
function allInstructions(tx, keys) {
    const inner = new Map();
    for (const g of tx.meta?.innerInstructions || []) inner.set(g.index, g.instructions);
    const out = [];
    tx.transaction.message.instructions.forEach((ix, i) => {
        out.push({program: keys[ix.programIdIndex], accounts: [...ix.accounts].map(a => keys[a]), data: Buffer.from(ix.data), outer: i, inner: -1});
        (inner.get(i) || []).forEach((x, j) => out.push({program: keys[x.programIdIndex], accounts: [...x.accounts].map(a => keys[a]), data: Buffer.from(x.data), outer: i, inner: j}));
    });
    return out;
}

/** События Anchor программы: внутренние инструкции самой программы с тегом события → {disc: hex 8 байт, body: Buffer, outer} */
function anchorEvents(ixs, program) {
    return ixs.filter(x => x.program === program && x.data.length >= 16 && x.data.subarray(0, 8).equals(EVENT_IX_TAG))
        .map(x => ({disc: x.data.subarray(8, 16).toString('hex'), body: x.data.subarray(16), outer: x.outer}));
}

/** Чтение Borsh по смещению */
class Reader {
    constructor(buf) { this.b = buf; this.o = 0; }
    u8() { return this.b.readUInt8(this.o++); }
    bool() { return this.u8() !== 0; }
    u64() { const v = this.b.readBigUInt64LE(this.o); this.o += 8; return v.toString(); }
    u128() { const lo = this.b.readBigUInt64LE(this.o), hi = this.b.readBigUInt64LE(this.o + 8); this.o += 16; return ((hi << 64n) + lo).toString(); }
    pubkey() { const v = bs58.encode(this.b.subarray(this.o, this.o + 32)); this.o += 32; return v; }
    string() { const n = this.b.readUInt32LE(this.o); this.o += 4; const v = this.b.subarray(this.o, this.o + n).toString('utf8'); this.o += n; return v; }
}

/** Общие поля транзакции для строки свопа: подпись, слот, время прихода, цена/лимит CU, число подписантов, переводы SOL (типсы) */
function txMeta(data) {
    const t = data.transaction.transaction;
    const keys = allKeys(t);
    let cu = {computeUnitPrice: null, computeUnitLimit: null};
    try { cu = getComputeUnitValues(t); } catch (e) {}
    const tips = [];
    for (const ix of t.transaction.message.instructions) {
        if (keys[ix.programIdIndex] !== '11111111111111111111111111111111') continue;
        const d = Buffer.from(ix.data);
        if (d.length >= 12 && d.readUInt32LE(0) === 2) tips.push({to: keys[ix.accounts[1]], sol: Number(d.readBigUInt64LE(4)) / 1e9});
    }
    return {
        signature: bs58.encode(t.signature),
        slot: Number(data.transaction.slot),
        ts: data.createdAt ? new Date(data.createdAt).getTime() || Date.now() : Date.now(),
        cuPrice: cu.computeUnitPrice ?? null,
        cuLimit: cu.computeUnitLimit ?? null,
        signers: t.transaction.signatures.length,
        tips: JSON.stringify(tips),
        failed: !!t.meta?.err,
        keys,
    };
}

/** Пакетная запись в MySQL: строки копятся и уходят одним INSERT по таймеру или при наборе пачки; flush() — перед выходом */
class BulkWriter {
    /** ignore — INSERT IGNORE (глушит и прочие ошибки строки); skipDuplicates — повтор уникального ключа пропускается (ON DUPLICATE KEY UPDATE id = id) */
    constructor(db, table, columns, {size = 200, everyMs = 2000, ignore = false, skipDuplicates = false} = {}) {
        Object.assign(this, {db, table, columns, size, buf: [], ignore});
        this.sql = `INSERT ${ignore ? 'IGNORE ' : ''}INTO ${table}(${columns.join(', ')}) VALUES ?${skipDuplicates ? ' ON DUPLICATE KEY UPDATE id = id' : ''}`;
        this.timer = setInterval(() => this.flush(), everyMs);
        this.written = 0;
    }
    push(row) { this.buf.push(this.columns.map(c => row[c] === undefined ? null : row[c])); if (this.buf.length >= this.size) this.flush(); }
    /** временные ошибки (взаимная блокировка, ожидание блокировки, обрыв соединения) — пачка возвращается в очередь, до 5 повторов;
     *  прочие — пачка теряется (ошибка в лог). Повтор не даёт дублей там, где есть уникальный ключ и skipDuplicates. */
    async flush() {
        if (!this.buf.length) return;
        const rows = this.buf; this.buf = [];
        try {
            await this.db.query(this.sql, [rows]);
            this.written += rows.length; this.retries = 0;
        } catch (e) {
            const transient = /Deadlock|Lock wait timeout|ECONNRESET|PROTOCOL_CONNECTION_LOST|ETIMEDOUT|ECONNREFUSED|closed state/i.test(`${e.code} ${e.message}`);
            if (transient && (this.retries || 0) < 5) {
                this.retries = (this.retries || 0) + 1;
                this.buf = rows.concat(this.buf);
                logger.warn(`${this.table}: ${e.message} — повтор ${this.retries}, строк ${rows.length}`);
            } else {
                this.retries = 0;
                logger.error(`${this.table}: ${e.message} — потеряно строк ${rows.length}`);
            }
        }
    }
    close() { clearInterval(this.timer); return this.flush(); }
}

/**
 * Поток gRPC по программам: onTx(data, filterNames) на каждую транзакцию; переподключение при ошибке/обрыве и при тишине дольше stallMs.
 * programs — {имя: programId} или {имя: {accountInclude, accountRequired}} (фильтр транзакций Yellowstone).
 * Неуспешные транзакции не приходят (failed: false). setFilters(programs) — заменить фильтры на лету (новый запрос в тот же поток).
 * onConnect(isReconnect) — после каждого подключения (например, дотянуть пропущенное за время обрыва).
 */
async function streamPrograms(programs, onTx, {stallMs = 60000, onConnect = null} = {}) {
    const {default: Client, CommitmentLevel} = require('@triton-one/yellowstone-grpc');
    let last = Date.now(), current = null, stopped = false, connected = false;
    const filter = f => typeof f === 'string' ? {accountInclude: [f], accountExclude: [], accountRequired: [], vote: false, failed: false}
        : {accountInclude: f.accountInclude || [], accountExclude: [], accountRequired: f.accountRequired || [], vote: false, failed: false};
    const build = p => ({
        accounts: {}, slots: {}, transactionsStatus: {}, blocks: {}, blocksMeta: {}, entry: {}, accountsDataSlice: [],
        transactions: Object.fromEntries(Object.entries(p).map(([k, f]) => [k, filter(f)])),
        commitment: CommitmentLevel.PROCESSED, includeTransactions: true, includeAccounts: false, includeEntries: false,
    });
    let request = build(programs);
    async function connect() {
        const client = new Client(process.env.WS_RPC, process.env.TRITON_ONE_TOKEN, {'grpc.keepalive_time_ms': 20000});
        await client.connect();
        const stream = await client.subscribe();
        current = stream;
        stream.on('data', d => { last = Date.now(); if (d.transaction) { try { onTx(d, d.filters || []); } catch (e) { logger.error(e.stack || e.message); } } });
        stream.on('error', e => { logger.error('поток', e.message); reconnect('ошибка'); });
        stream.on('end', () => reconnect('конец потока'));
        await new Promise((resolve, reject) => stream.write(request, e => e ? reject(e) : resolve()));
        logger.info(`подписка: ${Object.keys(request.transactions).join(', ')}`);
        if (onConnect) { try { onConnect(connected); } catch (e) { logger.error('onConnect', e.message); } }
        connected = true;
    }
    let reconnecting = false;
    async function reconnect(why) {
        if (reconnecting || stopped) return;
        reconnecting = true;
        logger.warn(`переподключение (${why})`);
        try { current && current.destroy && current.destroy(); } catch (e) {}
        for (let i = 0; ; i++) {
            await new Promise(r => setTimeout(r, Math.min(30000, 1000 * 2 ** i)));
            try { await connect(); last = Date.now(); break; } catch (e) { logger.error('подключение', e.message); }
        }
        reconnecting = false;
    }
    setInterval(() => { if (Date.now() - last > stallMs) reconnect(`тишина ${Math.round((Date.now() - last) / 1000)} с`); }, 10000);
    await connect();
    return {
        stop() { stopped = true; try { current && current.destroy && current.destroy(); } catch (e) {} },
        /** новые фильтры: сразу в текущий поток и для будущих переподключений */
        setFilters(p) {
            request = build(p);
            return new Promise(resolve => { try { current.write(request, e => { if (e) logger.error('смена фильтров', e.message); resolve(); }); } catch (e) { logger.error('смена фильтров', e.message); resolve(); } });
        },
    };
}

module.exports = {allKeys, allInstructions, anchorEvents, Reader, txMeta, BulkWriter, streamPrograms, EVENT_IX_TAG, logger};
