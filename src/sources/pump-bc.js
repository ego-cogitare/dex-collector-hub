/**
 * Источник pump.fun bonding curve (6EF8…): разбор событий сделок из логов транзакции, запись в MySQL (pump_fun_bc_swaps / pump_fun_bc_mints)
 * и раздача по каналу pump_bc.
 *
 * Разбор — перенос из copy-trade-solana/bot/utils.js (buildLoggedTransaction, PROGRAM_DATA_PARSERS: BC и CREATOR_CHANGE, detectTokenProgram):
 * событие TradeEvent (дискриминатор bddb7fd34ee661ee, «Program data: vdt/007mYe…») по той же схеме Borsh, хвост после cashback (выкуп,
 * валюта котировки не-SOL кривых и суммы/резервы в ней), смена криейтора (MigrateBondingCurveCreator, 9ba768dcd56cf303).
 *
 * Запись в MySQL — как в copy-trade-solana/sandbox/trader.js при LOG_SWAPS_HISTORY=1 и NONSOL_STORE=1 (те же строки, поля и SQL):
 *  - SOL-кривые: строка монеты при создании (CreateV2 + InitializeMint2), её сделки — PUMP_BC_SOL_WINDOW_MS (185 с) после создания;
 *    окно закрывается раньше, если первая сделка монеты — продажа (после 4-й сделки, как в трейдере); дубль — ключ подпись:vsr:user:amountSol;
 *    цена price_sol — по последней сделке транзакции, монета — последней сделки транзакции (как sage в трейдере);
 *    подпись — всегда подпись транзакции (исправлено относительно трейдера, см. parseLogs);
 *  - mayhem: монета, создание которой видели, и её сделки PUMP_BC_MAYHEM_WINDOW_MS (10 мин);
 *  - кривые не в SOL: все сделки (суммы в валюте котировки с её разрядностью), строка монеты — один раз за 30 мин.
 *
 * Раздача (канал pump_bc): одна запись на транзакцию — {sig, slot, ts (createdAt Triton, мс), recvTs, cuPrice, cuLimit, signers, tips,
 * create, tokenProgram, createData (base64 события создания с именем/символом/uri), tokens ({mint: {programId, decimals}}), swaps: [...]}; суммы и резервы — целые BigInt как в событии; смена криейтора — запись {creatorChange: {...}}.
 */
const bs58 = require('bs58').default;
const {deserialize} = require('borsh');
const {logger, BulkWriter} = require('../common');

const PROGRAM = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const WSOL = 'So11111111111111111111111111111111111111112';
const DISC_TRADE = 'bddb7fd34ee661ee', DISC_CREATOR_CHANGE = '9ba768dcd56cf303';
const COMPUTE_BUDGET = 'ComputeBudget111111111111111111111111111111';
const SOL_WINDOW_MS = +(process.env.PUMP_BC_SOL_WINDOW_MS || 185000);
const MAYHEM_WINDOW_MS = +(process.env.PUMP_BC_MAYHEM_WINDOW_MS || 600000);

const SWAP_STRUCT = {struct: {
    mint: {array: {type: 'u8', len: 32}}, sol_amount: 'u64', token_amount: 'u64', is_buy: 'u8', user: {array: {type: 'u8', len: 32}}, timestamp: 'i64',
    virtual_sol_reserves: 'u64', virtual_token_reserves: 'u64', real_sol_reserves: 'u64', real_token_reserves: 'u64',
    fee_recipient: {array: {type: 'u8', len: 32}}, fee_basis_points: 'u64', fee: 'u64', creator: {array: {type: 'u8', len: 32}},
    creator_fee_basis_points: 'u64', creator_fee: 'u64', track_volume: 'u8', total_unclaimed_tokens: 'u64', total_claimed_tokens: 'u64',
    current_sol_volume: 'u64', last_update_timestamp: 'u64', ix_name: 'string', mayhem_mode: 'u8', cashback_fee_basis_points: 'u64', cashback: 'u64',
}};
const CREATOR_CHANGE_STRUCT = {struct: {
    descriptor: {array: {type: 'u8', len: 8}}, mint: {array: {type: 'u8', len: 32}}, unknown2: {array: {type: 'u8', len: 32}},
    new_creator: {array: {type: 'u8', len: 32}}, old_creator: {array: {type: 'u8', len: 32}},
}};

/** TradeEvent → поля как в bot/utils.js (+ хвост: выкуп, валюта котировки, сумма и резервы в ней) */
function parseTrade(buffer) {
    const d = deserialize(SWAP_STRUCT, buffer.subarray(8));
    let off = 8 + 32 + 8 + 8 + 1 + 32 + 8 + 8 + 8 + 8 + 8 + 32 + 8 + 8 + 32 + 8 + 8 + 1 + 8 + 8 + 8 + 8;
    off += 4 + buffer.readUInt32LE(off) + 1 + 8 + 8;
    let quote = {quote_mint: null};
    if (buffer.length >= off + 76) {
        const qm = buffer.subarray(off + 20, off + 52);
        quote = {buyback_fee_basis_points: buffer.readBigUInt64LE(off), buyback_fee: buffer.readBigUInt64LE(off + 8), quote_mint: qm.some(b => b !== 0) ? bs58.encode(qm) : null,
            quote_amount: buffer.readBigUInt64LE(off + 52), virtual_quote_reserves: buffer.readBigUInt64LE(off + 60), real_quote_reserves: buffer.readBigUInt64LE(off + 68)};
    }
    return {...quote, mint: bs58.encode(d.mint), is_buy: !!d.is_buy, user: bs58.encode(d.user), time: Number(d.timestamp), amount: d.token_amount, total: d.sol_amount,
        fee_recipient: bs58.encode(d.fee_recipient), fee_basis_points: d.fee_basis_points, fee: d.fee, creator: bs58.encode(d.creator),
        creator_fee_basis_points: d.creator_fee_basis_points, creator_fee: d.creator_fee, virtual_sol_reserves: d.virtual_sol_reserves,
        virtual_token_reserves: d.virtual_token_reserves, real_sol_reserves: d.real_sol_reserves, real_token_reserves: d.real_token_reserves,
        ix_name: d.ix_name, mayhem_mode: d.mayhem_mode, cashback_fee_basis_points: d.cashback_fee_basis_points, cashback: d.cashback};
}

/** buildLoggedTransaction (только pump.fun BC): последний лог — success, события «vdt/007mYe…», в логах есть программа Token*; иначе null.
 *  Подпись каждой сделки — подпись транзакции (txSig). В bot/utils.js при нескольких подписантах событию i доставалась signatures[i]
 *  (подпись i-го подписанта, i — по всем событиям вместе с AMM), а при событиях больше, чем подписантов, транзакция терялась целиком (10.10, сверка хаба). */
function parseLogs(logs, txSig) {
    if (!logs.length || !/^Program\s\S+\ssuccess$/.test(logs[logs.length - 1])) return null;
    let tokenProgram = null; const data = [];
    for (const l of logs) {
        const m = l.match(/^Program\sdata:\s(vdt\/007mYe.+)$/);
        if (m) data.push(m[1]);
        if (!tokenProgram) { const t = l.match(/^Program\s(Token[^\s]+)/); if (t) tokenProgram = t[1]; }
    }
    if (!data.length || !tokenProgram) return null;
    const out = [];
    for (let i = 0; i < data.length; i++) {
        const buf = Buffer.from(data[i], 'base64');
        if (buf.subarray(0, 8).toString('hex') !== DISC_TRADE) continue;
        out.push({signature: txSig, ...parseTrade(buf)});
    }
    return out.length ? out : null;
}

/** detectCreatorChange из bot/utils.js */
function parseCreatorChange(logs) {
    for (let i = 0; i < logs.length; i++) {
        if (logs[i] !== 'Program log: Instruction: MigrateBondingCurveCreator') continue;
        for (let j = i + 1; j < logs.length; j++) {
            const m = logs[j].match(/^Program\sdata:\s(.+)$/);
            if (!m) continue;
            const buf = Buffer.from(m[1], 'base64');
            if (buf.subarray(0, 8).toString('hex') !== DISC_CREATOR_CHANGE) continue;
            /** с ~10.10 событие длиннее (176 байт): timestamp i64, mint, bonding_curve, sharing_config, old_creator, new_creator;
             *  старое (136): mint, ?, new_creator, old_creator. Новая раскладка по старым смещениям давала чужой mint — смена пропускалась */
            if (buf.length >= 176) {
                const k = o => bs58.encode(buf.subarray(o, o + 32));
                return {mint: k(16), old_creator: k(112), new_creator: k(144)};
            }
            const d = deserialize(CREATOR_CHANGE_STRUCT, buf);
            return {mint: bs58.encode(d.mint), old_creator: bs58.encode(d.old_creator), new_creator: bs58.encode(d.new_creator)};
        }
    }
    return null;
}

/** detectTokenProgram из bot/utils.js: программа, исполнившая InitializeMint2 */
function detectTokenProgram(logs) {
    let current = null;
    for (const l of logs) {
        const inv = l.match(/^Program\s(Token[^\s]+)\sinvoke/);
        if (inv) { current = inv[1]; continue; }
        if (current && l === 'Program log: Instruction: InitializeMint2') return current;
    }
    return null;
}

/** Ключи транзакции, CU и переводы SOL (findDirectTips трейдера: {from, to, sol}) */
function txInfo(tx) {
    const keys = tx.transaction.message.accountKeys.map(k => bs58.encode(k));
    for (const k of tx.meta?.loadedWritableAddresses || []) keys.push(bs58.encode(k));
    for (const k of tx.meta?.loadedReadonlyAddresses || []) keys.push(bs58.encode(k));
    let cuPrice = null, cuLimit = null; const tips = [];
    for (const ix of tx.transaction.message.instructions) {
        const pid = keys[ix.programIdIndex], d = Buffer.from(ix.data);
        if (pid === COMPUTE_BUDGET) { if (d.length === 9 && d[0] === 3) cuPrice = d.readBigUInt64LE(1); if (d[0] === 2) cuLimit = d.readUInt32LE(1); }
        else if (pid === '11111111111111111111111111111111' && d[0] === 2) tips.push({from: keys[ix.accounts[0]], to: keys[ix.accounts[1]], sol: Number(d.readBigUInt64LE(4)) / 1e9});
    }
    return {cuPrice: Number(cuPrice), cuLimit, tips};
}

const SWAP_COLUMNS = ['signature', 'mint', 'user', 'block', 'amount_sol', 'price_sol', 'vsr', 'vtr', 'rsr', 'rtr', 'amount_token', 'timestamp',
    'cu_price', 'cu_limit', 'protocol_fee', 'creator_fee', 'signers_count', 'tips'];

/** Источник: onTx(data) на каждую транзакцию потока с программой pump.fun; db — пул MySQL (null — без записи); publish(channel, record) */
function createPumpBc({db, write = true, publish = () => {}}) {
    const swapsW = new BulkWriter(db, 'pump_fun_bc_swaps', SWAP_COLUMNS, {size: 100, everyMs: 2000});
    if (!write) swapsW.push = () => {};
    const mintSql = (where) => db && write ? db.query(...where).catch(e => logger.error('pump_fun_bc_mints', e.message)) : null;
    const stats = {tx: 0, swaps: 0, creates: 0, written: 0, mayhem: 0, nonSol: 0, creatorChanges: 0, skipped: 0};
    const solLive = new Map();     // монета → {keys, n, firstSell} — окно записи SOL-сделок
    const mayhemLive = new Set();
    const qKeys = new Map();       // ключи дублей mayhem/не-SOL по монете (10 мин)
    const nonSolMintRows = new Set();
    const row = (sw, mint, slot, amt, price, vq, vt, createdAt, info, d, signers) => ({signature: sw.signature, mint, user: sw.user, block: slot, amount_sol: amt,
        price_sol: price, vsr: vq, vtr: vt, rsr: null, rtr: null, amount_token: Number(sw.amount) / 1e6, timestamp: createdAt, cu_price: info.cuPrice, cu_limit: info.cuLimit,
        protocol_fee: Number(sw.fee) / d, creator_fee: Number(sw.creator_fee) / d, signers_count: signers, tips: JSON.stringify(info.tips)});
    const dupQ = (mint, key) => {
        if (!qKeys.has(mint)) { qKeys.set(mint, new Set()); setTimeout(() => qKeys.delete(mint), 600000); }
        const s = qKeys.get(mint); if (s.has(key)) return true; s.add(key); return false;
    };
    const decimalsOf = (meta, q) => { const b = [...(meta.postTokenBalances || []), ...(meta.preTokenBalances || [])].find(x => x.mint === q); return b && b.uiTokenAmount ? Number(b.uiTokenAmount.decimals) : 6; };

    function onTx(data) {
        const recvTs = Date.now();
        const t = data.transaction.transaction, meta = t.meta, logs = meta.logMessages || [], signatures = t.transaction.signatures;
        const createdAt = data.createdAt ? new Date(data.createdAt).getTime() || recvTs : recvTs;
        const slot = Number(data.transaction.slot);
        stats.tx++;
        const cc = parseCreatorChange(logs);
        if (cc) {
            stats.creatorChanges++;
            const sig = bs58.encode(t.signature);
            publish('pump_bc', {sig, slot, ts: createdAt, recvTs, creatorChange: cc});
            /** смена криейтора кривой (почти всегда — разделение комиссий pfee… в первые 2 с после создания) */
            if (db && write) db.query('INSERT IGNORE INTO pump_fun_bc_creator_changes(mint, old_creator, new_creator, slot, ts, signature) VALUES (?, ?, ?, ?, ?, ?)',
                [cc.mint, cc.old_creator, cc.new_creator, slot, createdAt, sig]).catch(e => logger.error('pump_fun_bc_creator_changes', e.message));
            return;
        }
        let swaps;
        const txSig = bs58.encode(t.signature);
        try { swaps = parseLogs(logs, txSig); } catch (e) { stats.skipped++; return; }
        if (!swaps) { stats.skipped++; return; }
        stats.swaps += swaps.length;
        const info = txInfo(t);
        const create = logs.includes('Program log: Instruction: CreateV2') && logs.includes('Program log: Instruction: InitializeMint2');
        const tokenProgram = create ? detectTokenProgram(logs) : null;
        /** для ботов: событие создания с именем/символом/uri (base64 «Program data: G3KpTd7r…», разбор — parseMetadata бота) и токены транзакции
         *  (программа и разрядность по балансам — нужны сделкам кривых не в SOL) */
        const createLine = create ? logs.find(l => l.startsWith('Program data: G3KpTd7r')) : null;
        const tokens = {};
        for (const b of [...(meta.postTokenBalances || []), ...(meta.preTokenBalances || [])]) if (b.mint && !tokens[b.mint]) tokens[b.mint] = {programId: b.programId || null, decimals: b.uiTokenAmount ? Number(b.uiTokenAmount.decimals) : null};
        publish('pump_bc', {sig: txSig, slot, ts: createdAt, recvTs, cuPrice: info.cuPrice, cuLimit: info.cuLimit, signers: signatures.length, tips: info.tips,
            create, tokenProgram, createData: createLine ? createLine.slice(14) : null, tokens, swaps});
        if (!db || !write) return;
        const last = swaps[swaps.length - 1];

        /** mayhem: монета, создание которой видели, — MAYHEM_WINDOW_MS */
        if (last.mayhem_mode) {
            if (create && !mayhemLive.has(last.mint)) {
                mayhemLive.add(last.mint); setTimeout(() => mayhemLive.delete(last.mint), MAYHEM_WINDOW_MS);
                mintSql(['INSERT INTO pump_fun_bc_mints(mint, creator, mayhem_mode, created_at_ms, quote_mint) SELECT ?, ?, 1, ?, ? FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM pump_fun_bc_mints WHERE mint = ?)',
                    [last.mint, last.creator, createdAt, last.quote_mint || WSOL, last.mint]]);
            }
            for (const sw of swaps) {
                if (!sw.mayhem_mode || !sw.mint || !mayhemLive.has(sw.mint)) continue;
                const quote = !!sw.quote_mint;
                if (dupQ(sw.mint, `${sw.signature}:${sw.virtual_token_reserves}:${sw.user}:${quote ? sw.quote_amount : sw.total}`)) continue;
                const d = quote ? 10 ** decimalsOf(meta, sw.quote_mint) : 1e9;
                const vq = Number(quote ? sw.virtual_quote_reserves : sw.virtual_sol_reserves) / d, vt = Number(sw.virtual_token_reserves) / 1e6, amt = Number(quote ? sw.quote_amount : sw.total) / d;
                swapsW.push(row(sw, sw.mint, slot, sw.is_buy ? amt : -amt, vt ? vq / vt : null, vq, vt, createdAt, info, d, signatures.length)); stats.mayhem++;
            }
            return;
        }
        /** кривые не в SOL (виртуальные SOL-резервы нулевые): все сделки в валюте котировки, строка монеты раз в 30 мин */
        if (Number(last.virtual_sol_reserves) === 0) {
            for (const sw of swaps) {
                if (!sw.quote_mint) continue;
                if (dupQ(sw.mint, `${sw.signature}:${sw.virtual_token_reserves}:${sw.user}:${sw.quote_amount}`)) continue;
                const d = 10 ** decimalsOf(meta, sw.quote_mint);
                const vq = Number(sw.virtual_quote_reserves) / d, vt = Number(sw.virtual_token_reserves) / 1e6, amt = Number(sw.quote_amount) / d;
                swapsW.push(row(sw, sw.mint, slot, sw.is_buy ? amt : -amt, vt ? vq / vt : null, vq, vt, createdAt, info, d, signatures.length)); stats.nonSol++;
            }
            const first = swaps.find(x => x.quote_mint);
            if (first && !nonSolMintRows.has(first.mint)) {
                nonSolMintRows.add(first.mint); setTimeout(() => nonSolMintRows.delete(first.mint), 30 * 60000);
                mintSql(['INSERT INTO pump_fun_bc_mints(mint, creator, mayhem_mode, created_at_ms, quote_mint) SELECT ?, ?, ?, ?, ? FROM DUAL WHERE NOT EXISTS (SELECT 1 FROM pump_fun_bc_mints WHERE mint = ?)',
                    [first.mint, first.creator, first.mayhem_mode, createdAt, first.quote_mint, first.mint]]);
            }
            return;
        }
        /** SOL-кривые */
        const mint = last.mint;
        if (create && tokenProgram && !solLive.has(mint)) {
            stats.creates++;
            solLive.set(mint, {keys: new Set(), n: 0, firstSell: null});
            setTimeout(() => solLive.delete(mint), SOL_WINDOW_MS);
            mintSql(['INSERT INTO pump_fun_bc_mints(mint, creator, mayhem_mode, created_at_ms, quote_mint) VALUES (?, ?, ?, ?, ?)', [last.mint, last.creator, last.mayhem_mode, createdAt, WSOL]]);
        }
        const st = solLive.get(mint);
        if (!st) return;
        const price = Number(last.virtual_sol_reserves) / Number(last.virtual_token_reserves) / 1e3;
        for (const sw of swaps) {
            const amt = Number(sw.total) / 1e9, amountSol = sw.is_buy ? amt : -amt, vsr = Number(sw.virtual_sol_reserves) / 1e9;
            const key = sw.signature + ':' + vsr + ':' + sw.user + ':' + amountSol;
            if (st.keys.has(key)) continue;
            st.keys.add(key); st.n++; if (st.firstSell === null) st.firstSell = amountSol < 0;
            swapsW.push(row(sw, mint, slot, amountSol, price, vsr, Number(sw.virtual_token_reserves) / 1e6, createdAt, info, 1e9, signatures.length)); stats.written++;
        }
        /** как в трейдере: после 4-й сделки монета с первой сделкой-продажей перестаёт записываться */
        if (st.n > 3 && st.firstSell) solLive.delete(mint);
    }
    return {name: 'pump_bc', PROGRAM, onTx, stats, close: () => swapsW.close()};
}

module.exports = {createPumpBc, parseLogs, parseTrade, parseCreatorChange, detectTokenProgram, txInfo, PROGRAM, SWAP_COLUMNS};
