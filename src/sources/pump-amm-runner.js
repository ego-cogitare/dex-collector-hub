/**
 * Источник pump.fun AMM хаба: сделки в пулах монет, перешедших с бондинг-кривой pump.fun, в первые AMM_WINDOW_MIN (60) минут после миграции.
 * Перенесено из copy-trade-solana/collectors/amm-run.js; сделки и пулы — в MySQL (если write) и в каналы раздачи pump_amm / pump_amm_pool (publish).
 *   startPumpAmm({db, write, publish})   AMM_WINDOW_MIN=60  AMM_CONFIRM_MS=3000  AMM_FALLBACK_MS=30000
 *
 * Один поток gRPC (наш Triton), два фильтра:
 *   migrate — транзакции, где есть и программа кривой, и программа AMM: CreatePoolEvent → пул активен на окно (пулы, созданные вручную, не пишем);
 *   pools   — адреса активных пулов; при смене списка (не чаще раза в 5 с) открывается новый поток, старый закрывается, когда новый начал отдавать
 *             транзакции (перекрытие без пропуска; замена фильтров в том же потоке теряла сделки на несколько секунд).
 *             Новый поток открывает только новый пул (миграция). Пул с истёкшим окном сразу выходит из active (сделки не пишутся и не раздаются),
 *             а из фильтра уходит «под шумок» — при следующей смене потока из-за новой миграции; отдельная смена — только если истёкших в фильтре
 *             AMM_STALE_MAX (50) или старейший из них висит AMM_STALE_MIN (15) минут (миграций давно не было).
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
 * Окно — по времени сделки. Раз в минуту — {amm: …}: сделок за минуту, из них дотянуто (и доля), вызовов RPC; доля > 10% — предупреждение.
 */
const bs58 = require('bs58').default;
const {txMeta, BulkWriter, streamPrograms, logger} = require('../common');
const amm = require('./pump-amm');

const WINDOW_MS = Number(process.env.AMM_WINDOW_MIN || 60) * 60000;
const HTTP_URL = process.env.AMM_HTTP_RPC || process.env.RPC;   // дотягивание — наш Triton
const CONFIRM_MS = Number(process.env.AMM_CONFIRM_MS || 3000);     // RPC узнаёт подпись после подтверждения — ждём перед запросом
const FALLBACK_MS = Number(process.env.AMM_FALLBACK_MS || 30000);  // сделок из потока нет — дотягиваем по таймеру
const MAX_PAGES = 50;
const STALE_MAX = Number(process.env.AMM_STALE_MAX || 50);              // истёкших пулов в фильтре потока — сменить поток без новой миграции
const STALE_MS = Number(process.env.AMM_STALE_MIN || 15) * 60000;       // … или старейший истёкший висит в фильтре дольше

/** HTTP JSON-RPC: повтор с растущей паузой при 429 и сетевой ошибке */
async function rpc(method, params, tries = 4) {
    for (let i = 0; ; i++) {
        let r;
        try {
            r = await fetch(HTTP_URL, {method: 'POST', headers: {'content-type': 'application/json'}, signal: AbortSignal.timeout(30000),
                body: JSON.stringify({jsonrpc: '2.0', id: 1, method, params})});
        } catch (e) {
            if (i < tries) { await new Promise(res => setTimeout(res, 1000 * 2 ** i)); continue; }
            throw e;
        }
        if (r.status === 429 && i < tries) { await new Promise(res => setTimeout(res, 1000 * 2 ** i)); continue; }
        const j = await r.json();
        if (j.error) throw new Error(`${method}: ${JSON.stringify(j.error)}`);
        return j.result;
    }
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

/** Запуск на пуле MySQL db (таблицы уже созданы по schema.sql); write = false — без записи в MySQL (база нужна для восстановления пулов);
 *  publish(channel, record) — раздача. Возвращает {stop()} — отписка и запись буферов */
async function startPumpAmm({db, write = true, publish = () => {}}) {
    const swapsW = new BulkWriter(db, 'lp_pump_amm_swaps', [...amm.SWAP_COLUMNS, 'src'], {size: 500, skipDuplicates: true});
    const poolsW = new BulkWriter(db, 'lp_pump_amm_pools', amm.POOL_COLUMNS, {size: 20, ignore: true});
    if (!write) { swapsW.push = () => {}; poolsW.push = () => {}; logger.warn('pump_amm: запись в MySQL выключена'); }
    const swaps = {push: r => { swapsW.push(r); publish('pump_amm', r); }, close: () => swapsW.close()};
    const pools = {push: r => { poolsW.push(r); publish('pump_amm_pool', r); }, close: () => poolsW.close()};
    const stats = {migrations: 0, grpcTx: 0, skipped: 0, swaps: 0, backfilled: 0, rpcCalls: 0, reconnects: 0, filterUpdates: 0, staleRefreshes: 0, staleSubscribed: 0, errors: 0};
    const bfWhy = {};   // дотянутые сделки по причине: new — первые секунды пула, fallback — нет сделок из потока, reconnect, restore — после старта
    const err = (where, e) => { stats.errors++; if (stats.errors <= 20) logger.error(`${where}: ${e.message || e}`); };

    /** активные пулы: адрес → {mint, quote_mint, until, seen (ключи сделок), sigs (полученные подписи пула), migrationSig, lastSig, backfill} */
    const active = new Map();
    const poolInfo = pool => active.get(pool) || null;

    /** транзакция (поток или RPC) → пулы и сделки; src 0 — поток, 1 — RPC */
    function handleTx(d, src, why = null) {
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
        const r = amm.decode(d.transaction.transaction, meta, poolInfo);
        for (const p of r.pools) {
            const a = activate(p.pool, {mint: p.mint, quote_mint: p.quote_mint, is_mayhem: p.is_mayhem, createdAt: meta.ts, migrationSig: p.signature});
            if (!a) continue;
            pools.push(p); stats.migrations++;
            a.fallbackTimer = setTimeout(() => startBackfill(p.pool, {until: p.signature, delay: 0, why: 'fallback'}), FALLBACK_MS);
        }
        for (const s of r.swaps) {
            const a = active.get(s.pool);
            if (!a || s.ts > a.until) continue;   // окно — по времени сделки
            const key = `${s.signature}:${s.ev_index}`;   // как уникальный ключ в MySQL
            if (a.seen.has(key)) continue;
            a.seen.add(key);
            swaps.push({...s, src, is_mayhem: a.is_mayhem});   // is_mayhem — для фильтра раздачи, в MySQL не пишется
            stats.swaps++; if (src) { stats.backfilled++; bfWhy[why] = (bfWhy[why] || 0) + 1; }
            /** первая сделка из потока: пропущенное — между миграцией и ней */
            if (src === 0 && !a.backfill) startBackfill(s.pool, {until: a.migrationSig, before: s.signature, delay: CONFIRM_MS, why: 'new'});
        }
    }

    async function fetchTx(signature, why) {
        const r = await rpc('getTransaction', [signature, {encoding: 'json', maxSupportedTransactionVersion: 1, commitment: 'confirmed'}]);
        stats.rpcCalls++;
        if (r) handleTx(rpcToGrpc(r), 1, why);
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
        for (const x of all.slice().reverse()) {
            if (x.err || a.sigs.has(x.signature)) continue;
            a.sigs.add(x.signature);
            await fetchTx(x.signature, why).catch(e => { a.sigs.delete(x.signature); err('getTransaction', e); });
        }
    }

    function startBackfill(pool, {until, before, delay, why}) {
        const a = active.get(pool);
        if (!a || a.backfill) return;
        clearTimeout(a.fallbackTimer);
        a.backfill = new Promise(r => setTimeout(r, delay)).then(() => gapFill(pool, {until, before, why})).catch(e => err(`дотягивание ${pool}`, e));
    }

    /** после обрыва потока: пропуск по всем пулам, у которых уже есть граница (новые пулы закроет их первое дотягивание) */
    let catchUp = null, catchUpAgain = false;
    function scheduleCatchUp() {
        if (catchUp) { catchUpAgain = true; return; }
        catchUp = new Promise(r => setTimeout(r, CONFIRM_MS)).then(async () => {
            for (const [p, a] of active) if (a.lastSig && a.backfill) await gapFill(p, {until: a.lastSig, why: 'reconnect'}).catch(e => err(`дотягивание после смены подписки ${p}`, e));
        }).finally(() => { catchUp = null; if (catchUpAgain) { catchUpAgain = false; scheduleCatchUp(); } });
    }

    /** пулы в фильтре последнего открытого потока: истёкшие из них ждут следующей смены потока (stale: пул → когда истёк) */
    let subscribed = new Set();
    const stale = new Map();
    const filters = () => {
        const f = {migrate: {accountRequired: [amm.BONDING_CURVE, amm.PROGRAM]}};
        if (active.size) f.pools = {accountInclude: [...active.keys()]};
        subscribed = new Set(active.keys()); stale.clear();
        return f;
    };
    /** Смена списка пулов — новым потоком с перекрытием: старый закрывается, когда из нового пришла первая транзакция (или через 20 с).
     *  Замена фильтров в том же потоке (setFilters) теряла сделки на несколько секунд. Повторы из двух потоков отсекаются по подписи. */
    let stream = null, refreshTimer = null, switching = false, refreshAgain = false;
    const onTx = d => { stats.grpcTx++; try { handleTx(d, 0); } catch (e) { err('транзакция', e); } };
    const openStream = async () => {
        let firstData = null;
        const got = new Promise(r => { firstData = r; });
        const st = await streamPrograms(filters(), d => { firstData(); onTx(d); },
            {stallMs: 120000, onConnect: isReconnect => { if (isReconnect) { stats.reconnects++; scheduleCatchUp(); } }});
        return {st, got};
    };
    const refresh = () => {
        if (!stream) return;
        if (switching) { refreshAgain = true; return; }
        if (refreshTimer) return;
        refreshTimer = setTimeout(async () => {
            refreshTimer = null; switching = true;
            try {
                const next = await openStream();
                await Promise.race([next.got, new Promise(r => setTimeout(r, 20000))]);
                const old = stream; stream = next.st; old.stop();
                stats.filterUpdates++;
            } catch (e) { err('смена подписки', e); }
            switching = false;
            if (refreshAgain) { refreshAgain = false; refresh(); }
        }, 5000);
    };

    function activate(pool, {mint, quote_mint, is_mayhem = null, createdAt, migrationSig, lastSig}) {
        if (active.has(pool) || createdAt + WINDOW_MS <= Date.now()) return null;
        const a = {mint, quote_mint, is_mayhem, until: createdAt + WINDOW_MS, seen: new Set(), sigs: new Set(), migrationSig, lastSig, backfill: null};
        active.set(pool, a);
        refresh();
        return a;
    }

    /** после перезапуска: пулы моложе окна из базы, дотягивание после самой свежей (по слоту) записанной сделки или с миграции.
     *  Последняя по id строка не годится: это может быть дотянутая старая сделка — граница ушла бы в прошлое и всё после неё
     *  скачалось бы повторно (так появлялись дубли). Подписи записанных сделок последних ~150 слотов — в sigs: повторно не запрашиваются. */
    const [rows] = await db.query(`SELECT p.pool, p.mint, p.quote_mint, p.is_mayhem, p.created_at_ms, p.signature,
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
    for (const [p, until] of restored) gapFill(p, {until, why: 'restore'}).catch(e => err(`дотягивание после старта ${p}`, e));

    /** просроченные пулы (с запасом 30 с: сделки окна, дотягиваемые позже, ещё принимаются) — из active сразу, из фильтра потока —
     *  при следующей смене потока (новая миграция); свою смену потока — только если истёкших в фильтре много или висят давно */
    setInterval(() => {
        const now = Date.now();
        for (const [p, a] of active) if (a.until + 30000 <= now) { clearTimeout(a.fallbackTimer); active.delete(p); if (subscribed.has(p)) stale.set(p, now); }
        stats.staleSubscribed = stale.size;
        if (stale.size && (stale.size >= STALE_MAX || now - Math.min(...stale.values()) >= STALE_MS)) { stats.staleRefreshes++; refresh(); }
    }, 30000);

    /** статистика за минуту */
    let prev = {...stats};
    setInterval(() => {
        const d = Object.fromEntries(Object.keys(stats).map(k => [k, stats[k] - prev[k]]));
        prev = {...stats};
        const share = d.swaps ? +(d.backfilled / d.swaps * 100).toFixed(1) : 0;
        const line = {swapsMin: d.swaps, backfilledMin: d.backfilled, backfillPct: share, rpcMin: d.rpcCalls, grpcTxMin: d.grpcTx, skippedMin: d.skipped, migrationsMin: d.migrations,
            filterUpdatesMin: d.filterUpdates, staleRefreshesMin: d.staleRefreshes, staleSubscribed: stats.staleSubscribed, active: active.size,
            backfilledBy: {...bfWhy}, total: {swaps: stats.swaps, backfilled: stats.backfilled, reconnects: stats.reconnects, errors: stats.errors}};
        for (const k in bfWhy) delete bfWhy[k];
        if (share > 10 && d.swaps > 50) logger.warn({amm: 'много дотянуто', ...line}); else logger.info({amm: line});
    }, 60000);

    logger.info(`коллектор pump.fun AMM запущен: окно ${WINDOW_MS / 60000} мин, поток — gRPC, дотягивание — RPC ${HTTP_URL === process.env.RPC ? '(Triton)' : HTTP_URL}`);
    return {
        async stop() {
            stream.stop();
            await Promise.all([swaps.close(), pools.close()]);
        },
    };
}

module.exports = {startPumpAmm};
