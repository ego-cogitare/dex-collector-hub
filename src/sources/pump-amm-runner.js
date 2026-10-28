/**
 * Источник pump.fun AMM хаба: сделки в пулах монет, перешедших с бондинг-кривой pump.fun, в первые AMM_WINDOW_MIN (60) минут после миграции.
 * Перенесено из copy-trade-solana/collectors/amm-run.js; сделки и пулы — в MySQL (если write) и в каналы раздачи pump_amm / pump_amm_pool (publish).
 *   startPumpAmm({db, write, publish})   AMM_WINDOW_MIN=60  AMM_CONFIRM_MS=3000  AMM_FALLBACK_MS=30000
 *
 * Один поток gRPC (Triton или свой узел AMM_GRPC_URL / AMM_GRPC_TOKEN — Chainstack), два фильтра:
 *   migrate — транзакции, где есть и программа кривой, и программа AMM: CreatePoolEvent → пул активен на окно (пулы, созданные вручную, не пишем);
 *   pools   — адреса активных пулов; при смене списка (не чаще раза в 5 с) открывается новый поток, старый закрывается, когда новый начал отдавать
 *             транзакции (перекрытие без пропуска; замена фильтров в том же потоке теряла сделки на несколько секунд).
 *             Новый поток открывает только новый пул (миграция). Пул с истёкшим окном остаётся активным (сделки пишутся и раздаются — раз приходят,
 *             лишние не мешают) и уходит «под шумок» — при следующей смене потока из-за новой миграции; отдельная смена — только если истёкших
 *             AMM_STALE_MAX (50) или старейший из них истёк AMM_STALE_MIN (15) минут назад (миграций давно не было).
 * Сделки — события BuyEvent / SellEvent (emit_cpi во внутренних инструкциях: не обрезаются, в отличие от логов); CU, типсы, подписанты — из транзакции.
 *
 * Смена фильтров и переподключение потока дают короткие пропуски (WebSocket публичной ноды рвался под нагрузкой, gRPC без дотягивания
 * терял ~0.5% сделок). Дотягивание — тот же Triton по HTTP (RPC): getSignaturesForAddress постранично + getTransaction; транзакции из RPC
 * переводятся в форму gRPC и разбираются тем же кодом (src = 1, ts — время блока, секунды; у потока src = 0, ts — время прихода, мс):
 *   - новый пул: промежуток между миграцией (until) и первой сделкой из потока (before), через AMM_CONFIRM_MS после неё
 *     (RPC знает подпись только после подтверждения); сделок нет AMM_FALLBACK_MS — от миграции до текущего момента;
 *   - после обрыва и переподключения потока — по всем активным пулам всё после последней транзакции пула из потока (lastSig);
 *   - после перезапуска процесса — после последней записанной сделки пула.
 * Подписи, уже полученные из потока или RPC, повторно не запрашиваются; дубли сделок — по (signature, ev_index), он же уникальный ключ в MySQL.
 * Порядок внутри слота — tx_index (16.10): у потока — номер транзакции в блоке из gRPC (index); у дотянутых — по списку getSignaturesForAddress
 *   (внутри слота он ровно обратный порядку в блоке — сверено на 24 слотах): между соседями с известным номером (поток, границы until/before) — дробный,
 *   слот без известных номеров — место в слоте (0, 1, …), а если это самый свежий слот дотягивания без границы before — точные номера из getBlock (подписи).
 *   Сортировка сделок пула — (slot, tx_index, ev_index).
 * Окно — от миграции; после него пул пишется до ближайшей смены потока. Раз в минуту — {amm: …}: сделок за минуту, из них дотянуто (и доля), вызовов RPC; доля > 10% — предупреждение.
 *
 * AMM_ALL=1 (27.10, пользователь: «все свопы за всё время»): поток — один фильтр на всю программу AMM (смены потока нет), в MySQL — сделки всех пулов
 *   (монета пула вне окна — из инструкции свопа в той же транзакции, src 0). Окно AMM_WINDOW_MIN остаётся для пулов миграций: только они раздаются ботам
 *   (pump_amm / pump_amm_pool) и отдают транзакции сборщику «скрытых» движений. Дотягивания через RPC нет: пул в потоке с самой миграции,
 *   смены потока нет; на обрыве и перезапуске — потеря секунд (повтор сделки отсекает уникальный ключ MySQL). RPC — только добавка пула (getAccountInfo).
 */
const bs58 = require('bs58').default;
const {txMeta, BulkWriter, streamPrograms, grpcPool, logger} = require('../common');
const amm = require('./pump-amm');

const WINDOW_MS = Number(process.env.AMM_WINDOW_MIN || 60) * 60000;
const HTTP_URL = process.env.AMM_HTTP_RPC || process.env.RPC;   // дотягивание — наш Triton
/** узел gRPC потока AMM: AMM_GRPC_URL / AMM_GRPC_TOKEN (17.10 — под Chainstack: на тарифе 2 потока, ровно столько AMM держит при смене списка пулов); не заданы — Triton (WS_RPC) */
const GRPC_URL = process.env.AMM_GRPC_URL || process.env.WS_RPC, GRPC_TOKEN = process.env.AMM_GRPC_URL ? process.env.AMM_GRPC_TOKEN : process.env.TRITON_ONE_TOKEN;
/** одно соединение и не больше AMM_GRPC_MAX_STREAMS (2) потоков: 1 постоянный + 1 на время смены списка пулов / переподключения (18.10) */
const GRPC = grpcPool(GRPC_URL, GRPC_TOKEN, Number(process.env.AMM_GRPC_MAX_STREAMS || 2));
const CONFIRM_MS = Number(process.env.AMM_CONFIRM_MS || 3000);     // RPC узнаёт подпись после подтверждения — ждём перед запросом
const FALLBACK_MS = Number(process.env.AMM_FALLBACK_MS || 30000);  // сделок из потока нет — дотягиваем по таймеру
const MAX_PAGES = 50;
const STALE_MAX = Number(process.env.AMM_STALE_MAX || 50);              // истёкших пулов в фильтре потока — сменить поток без новой миграции
const STALE_MS = Number(process.env.AMM_STALE_MIN || 15) * 60000;       // … или старейший истёкший висит в фильтре дольше
const ALL = process.env.AMM_ALL === '1';   // все пулы программы (см. шапку)

/** HTTP JSON-RPC: не больше RPC_MAX запросов одновременно (AMM_RPC_MAX, 10); повтор с растущей паузой при 429, сетевой ошибке и превышении лимита
 *  в теле ответа (Chainstack: -32005 «exceeded the RPS limit», 18.10 — на рестарте хаб дотягивал сотню пулов разом и терял сделки) */
const RPC_MAX = Number(process.env.AMM_RPC_MAX || 10);
/** AMM_BACKFILL=0 — не дотягивать пропуски через RPC (18.10: после ночи без записи рестарт дотягивал часы истории по всем пулам и выедал квоту RPC) */
const NO_BACKFILL = process.env.AMM_BACKFILL === '0';
let rpcBusy = 0; const rpcWait = [];
const rpcSlot = () => rpcBusy < RPC_MAX ? (rpcBusy++, Promise.resolve()) : new Promise(r => rpcWait.push(r));
const rpcFree = () => { const next = rpcWait.shift(); if (next) next(); else rpcBusy--; };
/** AMM_RPC_RPS — не больше N запросов в секунду, ровным темпом (18.10: Chainstack Growth — 250/с на тариф, общий с трассировщиком фандинга 25/с → 200) */
const RPC_RPS = Number(process.env.AMM_RPC_RPS || 0); let rpcNextAt = 0;
const rpcPace = () => { if (!RPC_RPS) return null; const now = Date.now(), at = Math.max(now, rpcNextAt); rpcNextAt = at + 1000 / RPC_RPS; return at > now ? new Promise(r => setTimeout(r, at - now)) : null; };
/** Виртуальная добавка к SOL-резерву пула (24.10): u64 (лампорты) по смещению 245 аккаунта пула (длина 301) — пулы миграций нового образца торгуются так, будто к балансу
 *  SOL-счёта прибавлено ~17.58 SOL; события свопа дают реальный баланс без неё. «Сторожевое» значение (> 1e17) — 0; аккаунта нет / короче — null. Как tools/amm-pool-virtual.js. */
const VQ_OFF = 245, VQ_SENTINEL = 10n ** 17n;
function parseVirtualQuote(v) {
    if (!v || !v.data) return null;
    const b = Buffer.from(v.data[0], 'base64');
    if (b.length < VQ_OFF + 8) return null;
    const x = b.readBigUInt64LE(VQ_OFF);
    return x > VQ_SENTINEL ? 0n : x;
}
const rateLimited = e => e && (e.code === -32005 || e.code === -32429 || /rate|rps|limit|too many/i.test(e.message || ''));
async function rpc(method, params, tries = 6) {
    await rpcSlot();
    try {
        for (let i = 0; ; i++) {
            let r;
            const pace = rpcPace(); if (pace) await pace;
            try {
                r = await fetch(HTTP_URL, {method: 'POST', headers: {'content-type': 'application/json'}, signal: AbortSignal.timeout(30000),
                    body: JSON.stringify({jsonrpc: '2.0', id: 1, method, params})});
            } catch (e) {
                if (i < tries) { await new Promise(res => setTimeout(res, 500 * 2 ** i)); continue; }
                throw e;
            }
            if (r.status === 429 && i < tries) { await new Promise(res => setTimeout(res, 500 * 2 ** i)); continue; }
            const j = await r.json();
            if (j.error && rateLimited(j.error) && i < tries) { await new Promise(res => setTimeout(res, 500 * 2 ** i)); continue; }
            if (j.error) throw new Error(`${method}: ${JSON.stringify(j.error)}`);
            return j.result;
        }
    } finally { rpcFree(); }
}

/** Транзакция из RPC (getTransaction, encoding json) → форма gRPC, которую понимают txMeta / allInstructions / decode */
function rpcToGrpc(r) {
    const dec = s => bs58.decode(s);
    const ix = i => ({programIdIndex: i.programIdIndex, accounts: Uint8Array.from(i.accounts), data: dec(i.data)});
    const m = r.transaction.message, meta = r.meta || {};
    return {
        createdAt: r.blockTime ? r.blockTime * 1000 : Date.now(),
        transaction: {
            slot: r.slot,
            transaction: {
                signature: dec(r.transaction.signatures[0]),
                transaction: {signatures: r.transaction.signatures.map(dec), message: {accountKeys: m.accountKeys.map(dec), instructions: m.instructions.map(ix)}},
                meta: {
                    err: meta.err,
                    innerInstructions: (meta.innerInstructions || []).map(g => ({index: g.index, instructions: g.instructions.map(ix)})),
                    loadedWritableAddresses: (meta.loadedAddresses?.writable || []).map(dec),
                    loadedReadonlyAddresses: (meta.loadedAddresses?.readonly || []).map(dec),
                },
            },
        },
    };
}

/** Быстрый отсев до разбора: есть ли во внутренних инструкциях событие AMM (emit_cpi: тег + BuyEvent / SellEvent / CreatePoolEvent).
 *  Больше половины потока по пулам — транзакции ботов, которые только читают пул; полный разбор каждой упирал микросервер в 100% CPU. */
const EV_TAG = Buffer.from('e445a52e51cb9a1d', 'hex');
const EV_DISCS = ['67f4521f2cf57777', '3e2f370aa503dc2a', 'b1310cd2a076a774'].map(h => Buffer.from(h, 'hex'));
function hasAmmEvent(t) {
    for (const g of t.meta?.innerInstructions || []) for (const x of g.instructions) {
        const d = x.data;
        if (!d || d.length < 16 || d[0] !== 0xe4) continue;
        if (!EV_TAG.equals(Buffer.from(d.buffer, d.byteOffset, 8))) continue;
        const disc = Buffer.from(d.buffer, d.byteOffset + 8, 8);
        if (EV_DISCS.some(e => e.equals(disc))) return true;
    }
    return false;
}

/** Запуск на пуле MySQL db (таблицы уже созданы по sql/schema.sql — вручную); write = false — без записи в MySQL (база нужна для восстановления пулов);
 *  publish(channel, record) — раздача. Возвращает {stop()} — отписка и запись буферов */
async function startPumpAmm({db, write = true, publish = () => {}, publishPool = null}) {
    /** AMM_ALL: INSERT IGNORE вместо ON DUPLICATE KEY UPDATE id = id — 27.10 повтор сделки в пачке давал «Auto-increment value in UPDATE conflicts…» и терял всю пачку (300–480 строк) */
    const swapsW0 = new BulkWriter(db, 'lp_pump_amm_swaps', [...amm.SWAP_COLUMNS, 'src'], ALL ? {size: 500, ignore: true} : {size: 500, skipDuplicates: true});
    /** AMM_DB_SIG_LEN (27.10, пользователь): в MySQL — первые N символов подписи (12 ≈ 70 бит, как у Bitquery и в parquet; колонка CHAR(12) latin1_bin),
     *  ботам — полная. С укороченной подписью дотягивание после перезапуска не найдёт границу — держать AMM_BACKFILL_RESTORE=0 (с AMM_ALL дотягивания нет) */
    const DB_SIG = Number(process.env.AMM_DB_SIG_LEN || 0);
    const swapsW = DB_SIG > 0 ? {push: r => swapsW0.push({...r, signature: r.signature.slice(0, DB_SIG)}), close: () => swapsW0.close()} : swapsW0;
    const poolsW = new BulkWriter(db, 'lp_pump_amm_pools', amm.POOL_COLUMNS, {size: 20, ignore: true});
    if (!write) { swapsW.push = () => {}; poolsW.push = () => {}; logger.warn('pump_amm: запись в MySQL выключена'); }
    const swaps = {push: r => { swapsW.push(r); publish('pump_amm', r); }, close: () => swapsW.close()};
    /** AMM_ALL: сделка пула вне окна — только в MySQL; повторы из потока (та же подпись и номер события) — отсекаются (два поколения по 200 тыс. ключей) */
    let seenA = new Set(), seenB = new Set();
    const swapsAll = r => {
        const k = `${r.signature}:${r.ev_index}`;
        if (seenA.has(k) || seenB.has(k)) { stats.dupSkipped++; return; }
        seenA.add(k); if (seenA.size > 200000) { seenB = seenA; seenA = new Set(); }
        swapsW.push(r);
    };
    const pools = {push: r => { poolsW.push(r); publish('pump_amm_pool', r); }, close: () => poolsW.close()};
    const stats = {migrations: 0, grpcTx: 0, skipped: 0, swaps: 0, swapsOther: 0, dupSkipped: 0, backfilled: 0, rpcCalls: 0, reconnects: 0, filterUpdates: 0, staleRefreshes: 0, staleSubscribed: 0, errors: 0};
    const bfWhy = {};   // дотянутые сделки по причине: new — первые секунды пула, fallback — нет сделок из потока, reconnect, restore — после старта
    const err = (where, e) => { stats.errors++; if (stats.errors <= 20) logger.error(`${where}: ${e.message || e}`); };
    /** добавка пула → lp_pump_amm_pools.virtual_quote_reserves: аккаунт читаем после миграции (повторы — пока аккаунт не виден), строку обновляем, когда BulkWriter её записал */
    const vqStats = {ok: 0, none: 0, fail: 0};
    const saveVirtualQuote = async pool => {
        let v; let got = false;
        for (let i = 0; i < 5 && !got; i++) {
            try { const r = await rpc('getAccountInfo', [pool, {encoding: 'base64', commitment: 'confirmed'}]); stats.rpcCalls++; if (r?.value) { v = parseVirtualQuote(r.value); got = true; } }
            catch (e) { if (i === 4) { vqStats.fail++; return err('добавка пула', e); } }
            if (!got) await new Promise(r => setTimeout(r, 1000 * (i + 1)));
        }
        if (!got || v === null) { vqStats.none++; return; }
        for (let i = 0; i < 8; i++) {
            await new Promise(r => setTimeout(r, i === 0 ? 0 : 5000));
            try {
                const [res] = await db.query('UPDATE lp_pump_amm_pools SET virtual_quote_reserves = ? WHERE pool = ?', [v.toString(), pool]);
                if (res.affectedRows > 0 || res.changedRows > 0) { vqStats.ok++; return; }
            } catch (e) { vqStats.fail++; return err('добавка пула: запись', e); }
        }
        vqStats.fail++; err('добавка пула', new Error(`${pool}: строки пула нет в MySQL 35 с`));
    };

    /** активные пулы: адрес → {mint, quote_mint, until, seen (ключи сделок), sigs (полученные подписи пула), migrationSig, lastSig, backfill} */
    const active = new Map();
    const poolInfo = pool => active.get(pool) || null;
    /** номера транзакций потока в блоке: подпись → {slot, idx} (опоры для номеров дотянутых); последние ~AMM_TXIDX_KEEP (20 тыс. ≈ 2 мин потока) —
     *  опоры нужны только у свежих промежутков (миграция и первая сделка пула — секунды назад). 250 тыс. раздували кучу хаба на ~500 МБ (16.10 07:50 — падение по памяти) */
    const TXIDX_KEEP = Number(process.env.AMM_TXIDX_KEEP || 20000);
    const txIdx = new Map();
    const rememberIdx = (sig, slot, idx) => {
        txIdx.set(sig, {slot, idx});
        if (txIdx.size > TXIDX_KEEP * 1.2) { let n = 0; const drop = txIdx.size - TXIDX_KEEP; for (const k of txIdx.keys()) { txIdx.delete(k); if (++n >= drop) break; } }
    };

    /** транзакция (поток или RPC) → пулы и сделки; src 0 — поток, 1 — RPC; txIndex — порядок в блоке (у потока — из gRPC) */
    function handleTx(d, src, why = null, txIndex = null) {
        if (src === 0) {
            const ti = d.transaction.transaction.index;
            if (ti !== undefined && ti !== null) {
                txIndex = Number(ti);
                if (!ALL) rememberIdx(bs58.encode(d.transaction.transaction.signature), Number(d.transaction.slot), txIndex);   // и транзакции без событий AMM — опоры для дотянутых
            }
        }
        if (src === 0 && !hasAmmEvent(d.transaction.transaction)) { stats.skipped++; return; }
        const meta = txMeta(d);
        if (meta.failed) return;
        /** подпись — известна всем активным пулам, которые есть в транзакции (граница дотягивания) */
        for (const k of meta.keys) {
            const a = active.get(k);
            if (!a) continue;
            a.sigs.add(meta.signature);
            if (src === 0) a.lastSig = meta.signature;
        }
        const r = amm.decode(d.transaction.transaction, meta, poolInfo, {all: ALL});
        for (const p of r.pools) {
            const a = activate(p.pool, {mint: p.mint, quote_mint: p.quote_mint, is_mayhem: p.is_mayhem, createdAt: meta.ts, migrationSig: p.signature});
            if (!a) continue;
            pools.push(p); stats.migrations++;
            if (write) saveVirtualQuote(p.pool);
            if (!ALL) a.fallbackTimer = setTimeout(() => startBackfill(p.pool, {until: p.signature, delay: 0, why: 'fallback'}), FALLBACK_MS);
        }
        for (const s of r.swaps) {
            if (publishPool && src === 0) poolSwap(s, txIndex, d.transaction.transaction.meta);
            const a = active.get(s.pool);
            if (!a) { if (ALL && src === 0) { swapsAll({...s, src, tx_index: txIndex}); stats.swapsOther++; } continue; }   // пул в подписке — пишем, в т.ч. после окна (до ближайшей смены потока)
            const key = `${s.signature}:${s.ev_index}`;   // как уникальный ключ в MySQL
            if (a.seen.has(key)) continue;
            a.seen.add(key);
            swaps.push({...s, src, tx_index: txIndex, is_mayhem: a.is_mayhem});   // is_mayhem — для фильтра раздачи, в MySQL не пишется
            stats.swaps++; if (src) { stats.backfilled++; bfWhy[why] = (bfWhy[why] || 0) + 1; }
            /** первая сделка из потока: пропущенное — между миграцией и ней */
            if (src === 0 && !a.backfill) startBackfill(s.pool, {until: a.migrationSig, before: s.signature, delay: CONFIRM_MS, why: 'new'});
        }
    }

    /** amm_pool (27.10): сделка пула клиентам, которые ждут этот пул (хаб: server.publishPool) — резервы ПОСЛЕ сделки = балансы счетов пула после транзакции
     *  (postTokenBalances, владелец — пул; резерв в событии равен балансу счёта). Запасной путь — по событию: buy/sell — в пул quote_amount + lp_fee / из пула
     *  quote_amount − lp_fee; у *_v2 комиссии создателя и buyback остаются на счёте пула до сбора (SweepCreatorFee / SweepProtocolFee) — сверено 27.10 */
    function poolSwap(s, txIndex, meta) {
        try {
            let q1 = null, b1 = null;
            for (const t of meta?.postTokenBalances || []) {
                if (t.owner !== s.pool) continue;
                if (t.mint === s.quote_mint) q1 = BigInt(t.uiTokenAmount.amount); else if (t.mint === s.mint) b1 = BigInt(t.uiTokenAmount.amount);
            }
            if (q1 === null || b1 === null) {
                const q0 = BigInt(s.pool_quote_reserves), b0 = BigInt(s.pool_base_reserves), qa = BigInt(s.quote_amount), ba = BigInt(s.base_amount), lp = BigInt(s.lp_fee || 0);
                const kept = /_v2$/.test(s.ix_name || '') ? BigInt(s.coin_creator_fee || 0) + BigInt(s.buyback_fee || 0) : 0n;
                q1 = s.is_buy ? q0 + qa + lp + kept : q0 - qa + lp; b1 = s.is_buy ? b0 - ba : b0 + ba;
            }
            /** qm / mint / qa / fe (29.10) — для подписки по котировке: котировка и база пула, quote_amount и комиссии сделки (lp + protocol + coin_creator) */
            publishPool({pool: s.pool, slot: s.slot, tx: txIndex, ts: s.ts, buy: s.is_buy ? 1 : 0, q1: Number(q1), b1: Number(b1), user: s.user, sig: s.signature,
                qm: s.quote_mint, mint: s.mint, qa: Number(s.quote_amount), fe: Number(s.lp_fee || 0) + Number(s.protocol_fee || 0) + Number(s.coin_creator_fee || 0)});
        } catch (e) { err('amm_pool', e); }
    }

    async function fetchTx(signature, why, txIndex = null) {
        const r = await rpc('getTransaction', [signature, {encoding: 'json', maxSupportedTransactionVersion: 1, commitment: 'confirmed'}]);
        stats.rpcCalls++;
        if (r) handleTx(rpcToGrpc(r), 1, why, txIndex);
    }

    /** номера в блоке для подписей дотягивания: chron — подписи пула от старых к новым (getSignaturesForAddress, развёрнутый), until/before — границы промежутка */
    async function orderIndex(chron, {until, before}) {
        const out = new Map();
        const known = sig => { const k = txIdx.get(sig); return k ? k.idx : undefined; };
        const bySlot = [];
        for (const x of chron) { const last = bySlot[bySlot.length - 1]; if (last && last.slot === x.slot) last.sigs.push(x.signature); else bySlot.push({slot: x.slot, sigs: [x.signature]}); }
        for (let g = 0; g < bySlot.length; g++) {
            const {slot, sigs} = bySlot[g];
            const u = until && txIdx.get(until), b = before && txIdx.get(before);
            const seq = [...(u && u.slot === slot ? [{sig: until, v: u.idx}] : []), ...sigs.map(sig => ({sig, v: known(sig)})), ...(b && b.slot === slot ? [{sig: before, v: b.idx}] : [])];
            if (seq.every(e => e.v === undefined)) {
                if (g === bySlot.length - 1 && !before) {   // самый свежий слот без границы: в него могут прийти сделки потока — точные номера из блока
                    try {
                        const blk = await rpc('getBlock', [slot, {transactionDetails: 'signatures', rewards: false, maxSupportedTransactionVersion: 0, commitment: 'confirmed'}]);
                        stats.rpcCalls++;
                        const pos = new Map(blk.signatures.map((s2, i) => [s2, i]));
                        for (const e of seq) if (pos.has(e.sig)) e.v = pos.get(e.sig);
                    } catch (e) { err(`getBlock ${slot}`, e); }
                }
                if (seq.every(e => e.v === undefined)) seq.forEach((e, i) => { e.v = i; });
            }
            /** пробелы между известными: (L, R) — равномерно; только L — (L, L + 1); только R — (R − 1, R) */
            for (let i = 0; i < seq.length;) {
                if (seq[i].v !== undefined) { i++; continue; }
                let j = i; while (j < seq.length && seq[j].v === undefined) j++;
                const L = i > 0 ? seq[i - 1].v : undefined, R = j < seq.length ? seq[j].v : undefined, m = j - i;
                for (let k = 1; k <= m; k++) {
                    seq[i + k - 1].v = L !== undefined && R !== undefined ? L + (R - L) * k / (m + 1) : L !== undefined ? L + k / (m + 1) : R - 1 + k / (m + 1);
                }
                i = j;
            }
            for (const e of seq) out.set(e.sig, e.v);
        }
        return out;
    }

    /** одна страница подписей; нода может не знать подпись границы (Transaction … not found): before — повторы, затем без него;
     *  until (транзакция из потока, не попавшая в блок) — граница отодвигается к миграции */
    async function sigPage(pool, a, opts) {
        for (let i = 0; ; i++) {
            try { const r = await rpc('getSignaturesForAddress', [pool, opts]); stats.rpcCalls++; return r || []; }
            catch (e) {
                if (!/not found/.test(e.message)) throw e;
                if (opts.before && i < 3) { await new Promise(r => setTimeout(r, 3000)); continue; }
                if (opts.before) { delete opts.before; continue; }
                if (opts.until && opts.until !== a.migrationSig) { opts.until = a.migrationSig; continue; }
                throw e;
            }
        }
    }

    /** все подписи пула в промежутке (until, before) постранично → транзакции, которых ещё не видели (от старых к новым) */
    async function gapFill(pool, {until, before, why}) {
        if (NO_BACKFILL) return;   // AMM_BACKFILL=0 — дотягивание через RPC выключено
        const a = active.get(pool);
        if (!a) return;
        const opts = {limit: 1000, commitment: 'confirmed'};
        if (until) opts.until = until;
        if (before) opts.before = before;
        const all = [];
        for (let page = 0; page < MAX_PAGES; page++) {
            const sigs = await sigPage(pool, a, opts);
            all.push(...sigs);
            if (sigs.length < 1000) break;
            opts.before = sigs.at(-1).signature;
            if (page === MAX_PAGES - 1) logger.warn(`дотягивание ${pool}: больше ${MAX_PAGES * 1000} подписей, взяты последние`);
        }
        const chron = all.slice().reverse();
        const order = await orderIndex(chron, {until, before});
        for (const x of chron) {
            if (x.err || a.sigs.has(x.signature)) continue;
            a.sigs.add(x.signature);
            await fetchTx(x.signature, why, order.has(x.signature) ? order.get(x.signature) : null).catch(e => { a.sigs.delete(x.signature); err('getTransaction', e); });
        }
    }

    function startBackfill(pool, {until, before, delay, why}) {
        if (ALL) return;   // AMM_ALL: дотягивания нет (см. шапку)
        const a = active.get(pool);
        if (!a || a.backfill) return;
        clearTimeout(a.fallbackTimer);
        a.backfill = new Promise(r => setTimeout(r, delay)).then(() => gapFill(pool, {until, before, why})).catch(e => err(`дотягивание ${pool}`, e));
    }

    /** после обрыва потока: пропуск по всем пулам, у которых уже есть граница (новые пулы закроет их первое дотягивание) */
    let catchUp = null, catchUpAgain = false;
    function scheduleCatchUp() {
        if (ALL) return;
        if (catchUp) { catchUpAgain = true; return; }
        catchUp = new Promise(r => setTimeout(r, CONFIRM_MS)).then(async () => {
            for (const [p, a] of active) if (a.lastSig && a.backfill) await gapFill(p, {until: a.lastSig, why: 'reconnect'}).catch(e => err(`дотягивание после смены подписки ${p}`, e));
        }).finally(() => { catchUp = null; if (catchUpAgain) { catchUpAgain = false; scheduleCatchUp(); } });
    }

    /** истёкшие пулы (окно + 30 с прошло) ещё в подписке: пул → когда истёк; убираются из active при открытии следующего потока */
    const stale = new Map();
    const filters = () => {
        for (const p of stale.keys()) { const a = active.get(p); if (a) clearTimeout(a.fallbackTimer); active.delete(p); activeB64.delete(Buffer.from(bs58.decode(p)).toString('base64')); }
        stale.clear();
        if (ALL) return {all: {accountInclude: [amm.PROGRAM]}};   // все транзакции программы: и миграции, и сделки любых пулов
        const f = {migrate: {accountRequired: [amm.BONDING_CURVE, amm.PROGRAM]}};
        /** 24.10: Chainstack не принимает > 200 аккаунтов в одном фильтре (с 16:24 UTC, когда пулов в окне 300 мин стало 201, каждая смена подписки падала
         *  «stream receive failed», новые пулы не подписывались) — пулы делим на фильтры по AMM_POOLS_CHUNK (150) */
        const CH = Number(process.env.AMM_POOLS_CHUNK || 150), ps = [...active.keys()];
        for (let i = 0; i < ps.length; i += CH) f[i ? `pools${i / CH}` : 'pools'] = {accountInclude: ps.slice(i, i + CH)};
        return f;
    };
    /** Смена списка пулов — новым потоком с перекрытием: старый закрывается через AMM_SWITCH_OVERLAP_MS (10 с) после первой транзакции из нового (или новый — через 20 с без данных).
     *  Замена фильтров в том же потоке (setFilters) теряла сделки на несколько секунд. Повторы из двух потоков отсекаются по подписи.
     *  20.10: закрытие старого сразу после первой транзакции нового теряло сделки, ещё не отданные отстающим старым потоком (новый начинает с текущего слота) —
     *  продажа красавца в пуле Fndy2d… 18:13:52 UTC пропала (смена потока 18:13:57), бот вошёл, считая его в позиции. */
    const SWITCH_OVERLAP_MS = Number(process.env.AMM_SWITCH_OVERLAP_MS || 10000);
    let stream = null, refreshTimer = null, switching = false, refreshAgain = false;
    const hidden = require('../hidden-sink');   // «скрытые» движения SOL — общий отправитель с основным потоком (повторы по подписи отсекаются)
    /** AMM_ALL: сборщику «скрытых» движений — как раньше, только транзакции пулов окна (иначе поток всей программы); ключи сверяются в base64 — дешевле bs58 */
    const activeB64 = new Set();
    const touchesActive = d => {
        const t = d.transaction.transaction, m = t.transaction?.message, meta = t.meta;
        for (const L of [m?.accountKeys, meta?.loadedWritableAddresses, meta?.loadedReadonlyAddresses]) for (const k of L || []) if (activeB64.has(Buffer.from(k).toString('base64'))) return true;
        return false;
    };
    const onTx = d => { stats.grpcTx++; try { handleTx(d, 0); } catch (e) { err('транзакция', e); } if (hidden.ON && (!ALL || touchesActive(d))) setImmediate(() => hidden.push(d)); };
    const openStream = async () => {
        let firstData = null;
        const got = new Promise(r => { firstData = r; });
        const st = await streamPrograms(filters(), d => { firstData(); onTx(d); },
            {stallMs: Number(process.env.AMM_STALL_MS || 15000), pool: GRPC, onConnect: isReconnect => { if (isReconnect) { stats.reconnects++; scheduleCatchUp(); } }});
        return {st, got};
    };
    const refresh = () => {
        if (!stream || ALL) return;   // AMM_ALL: фильтр на всю программу — смена потока не нужна
        if (switching) { refreshAgain = true; return; }
        if (refreshTimer) return;
        refreshTimer = setTimeout(async () => {
            refreshTimer = null; switching = true;
            let retry = false;
            try {
                const next = await openStream();   // при занятом потолке потоков — исключение, прежний поток работает дальше
                const ok = await Promise.race([next.got.then(() => true), new Promise(r => setTimeout(() => r(false), 20000))]);
                if (ok) { const old = stream; stream = next.st; stats.filterUpdates++; await new Promise(r => setTimeout(r, SWITCH_OVERLAP_MS)); old.stop(); }
                else { next.st.stop(); retry = true; err('смена подписки', new Error('новый поток 20 с без данных — оставлен прежний')); }
            } catch (e) { retry = true; err('смена подписки', e); }
            switching = false;
            if (refreshAgain) { refreshAgain = false; refresh(); }
            else if (retry) setTimeout(refresh, 30000);   // не чаще раза в 30 с, пока узел отказывает
        }, 5000);
    };

    function activate(pool, {mint, quote_mint, is_mayhem = null, createdAt, migrationSig, lastSig}) {
        if (active.has(pool) || createdAt + WINDOW_MS <= Date.now()) return null;
        const a = {mint, quote_mint, is_mayhem, until: createdAt + WINDOW_MS, seen: new Set(), sigs: new Set(), migrationSig, lastSig, backfill: null};
        active.set(pool, a); activeB64.add(Buffer.from(bs58.decode(pool)).toString('base64'));
        refresh();
        return a;
    }

    /** после перезапуска: пулы моложе окна из базы, дотягивание после самой свежей (по слоту) записанной сделки или с миграции.
     *  Последняя по id строка не годится: это может быть дотянутая старая сделка — граница ушла бы в прошлое и всё после неё
     *  скачалось бы повторно (так появлялись дубли). Подписи записанных сделок последних ~150 слотов — в sigs: повторно не запрашиваются. */
    /** AMM_ALL: дотягивания нет — сделки пулов при старте не нужны, только список пулов окна (28.10: ~500 запросов к таблице всех сделок AMM, ~3 мин без потока) */
    const [rows] = ALL ? await db.query('SELECT pool, mint, quote_mint, is_mayhem, created_at_ms, signature, NULL last_sig, NULL last_slot FROM lp_pump_amm_pools WHERE created_at_ms > ?', [Date.now() - WINDOW_MS])
        : await db.query(`SELECT p.pool, p.mint, p.quote_mint, p.is_mayhem, p.created_at_ms, p.signature,
            (SELECT s.signature FROM lp_pump_amm_swaps s WHERE s.pool = p.pool ORDER BY s.slot DESC, s.id DESC LIMIT 1) last_sig,
            (SELECT MAX(s.slot) FROM lp_pump_amm_swaps s WHERE s.pool = p.pool) last_slot
        FROM lp_pump_amm_pools p WHERE p.created_at_ms > ?`, [Date.now() - WINDOW_MS]);
    const restored = [];
    for (const r of rows) {
        const a = activate(r.pool, {mint: r.mint, quote_mint: r.quote_mint, is_mayhem: r.is_mayhem, createdAt: Number(r.created_at_ms), migrationSig: r.signature, lastSig: r.last_sig});
        if (!a) continue;
        a.backfill = true;
        if (r.last_slot) {
            const [recent] = await db.query('SELECT DISTINCT signature FROM lp_pump_amm_swaps WHERE pool = ? AND slot >= ?', [r.pool, Number(r.last_slot) - 150]);
            for (const x of recent) a.sigs.add(x.signature);
        }
        restored.push([r.pool, r.last_sig || r.signature]);
    }
    logger.info(`активных пулов из базы: ${active.size}`);

    stream = (await openStream()).st;
    /** AMM_BACKFILL_RESTORE=0 — не дотягивать историю пулов после старта процесса (18.10: после ночи без записи в MySQL — часы истории, ~5 900 запросов/мин подряд) */
    if (!ALL && process.env.AMM_BACKFILL_RESTORE !== '0') for (const [p, until] of restored) gapFill(p, {until, why: 'restore'}).catch(e => err(`дотягивание после старта ${p}`, e));

    /** просроченные пулы (окно + 30 с) — пишутся дальше и уходят при следующей смене потока (новая миграция);
     *  свою смену потока — только если истёкших много или старейший истёк давно */
    setInterval(() => {
        const now = Date.now();
        if (ALL) {   // поток не меняется: пул с истёкшим окном (+30 с) сразу выходит из окна — дальше пишется как прочие
            for (const [p, a] of active) if (a.until + 30000 <= now) { clearTimeout(a.fallbackTimer); active.delete(p); activeB64.delete(Buffer.from(bs58.decode(p)).toString('base64')); }
            return;
        }
        for (const [p, a] of active) if (a.until + 30000 <= now && !stale.has(p)) stale.set(p, now);
        stats.staleSubscribed = stale.size;
        if (stale.size && (stale.size >= STALE_MAX || now - Math.min(...stale.values()) >= STALE_MS)) { stats.staleRefreshes++; refresh(); }
    }, 30000);

    /** статистика за минуту */
    let prev = {...stats};
    setInterval(() => {
        const d = Object.fromEntries(Object.keys(stats).map(k => [k, stats[k] - prev[k]]));
        prev = {...stats};
        const share = d.swaps ? +(d.backfilled / d.swaps * 100).toFixed(1) : 0;
        const line = {swapsMin: d.swaps, ...(ALL ? {swapsOtherMin: d.swapsOther, dupSkippedMin: d.dupSkipped} : {}), backfilledMin: d.backfilled, backfillPct: share, rpcMin: d.rpcCalls, grpcTxMin: d.grpcTx, skippedMin: d.skipped, migrationsMin: d.migrations,
            filterUpdatesMin: d.filterUpdates, staleRefreshesMin: d.staleRefreshes, staleSubscribed: stats.staleSubscribed, active: active.size, grpcStreams: GRPC.open,
            backfilledBy: {...bfWhy}, total: {swaps: stats.swaps, backfilled: stats.backfilled, reconnects: stats.reconnects, errors: stats.errors, virtualQuote: {...vqStats}}};
        for (const k in bfWhy) delete bfWhy[k];
        if (!require('worker_threads').isMainThread && hidden.ON) line.hidden = hidden.stats();   // в потоке AMM — свой отправитель «скрытых» движений
        if (share > 10 && d.swaps > 50) logger.warn({amm: 'много дотянуто', ...line}); else logger.info({amm: line});
    }, 60000);

    logger.info(`коллектор pump.fun AMM запущен${ALL ? ' (AMM_ALL: все пулы программы, раздача ботам — окно, без дотягивания)' : ''}: окно ${WINDOW_MS / 60000} мин, поток — gRPC, дотягивание — RPC ${HTTP_URL === process.env.RPC ? '(Triton)' : HTTP_URL}`);
    return {
        async stop() {
            stream.stop();
            await Promise.all([swaps.close(), pools.close()]);
        },
    };
}

module.exports = {startPumpAmm};
