/**
 * pump.fun AMM (PumpSwap, pAMMBay…): сделки в пулах монет, перешедших с бондинг-кривой pump.fun, — первые 60 минут после миграции
 * (окно — в collectors/amm-run.js). IDL — из блокчейна (версия 0.1.0, 08.10.2026), события — emit_cpi.
 *
 * Пул миграции: CreatePoolEvent в транзакции, где участвует программа кривой pump.fun (6EF8…, инструкция migrate);
 * пулы, созданные вручную (create_pool без кривой), не пишем.
 * Сделка: BuyEvent / SellEvent — резервы пула (pool_*_token_reserves — до сделки), суммы, комиссии (lp, протокол, создатель, cashback, buyback),
 * пользователь; монету берём из пула (base_mint из CreatePoolEvent).
 * Покупка (BuyEvent): base_amount — получено токенов, quote_amount — без комиссий, user_quote_amount — списано у пользователя всего
 * (в событиях buy_exact_quote_in, buy_exact_quote_in_v2, multi_hop_swap программа пишет их наоборот — parseBuy меняет местами по равенству сумм);
 * продажа (SellEvent): base_amount — продано токенов, quote_amount — quote_amount_out (до комиссий), user_quote_amount — получено пользователем.
 */
const {allInstructions, anchorEvents, Reader} = require('../common');

const PROGRAM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
const BONDING_CURVE = '6EF8rrecthR5Dkzon8Nwu78hRvfCKubJ14M5uBEwF6P';
const EV_BUY = '67f4521f2cf57777', EV_SELL = '3e2f370aa503dc2a', EV_CREATE_POOL = 'b1310cd2a076a774';

const SWAP_COLUMNS = ['signature', 'ev_index', 'slot', 'ts', 'event_ts', 'pool', 'mint', 'quote_mint', 'user', 'is_buy', 'ix_name',
    'base_amount', 'quote_amount', 'user_quote_amount', 'limit_quote_amount', 'pool_base_reserves', 'pool_quote_reserves',
    'lp_fee', 'protocol_fee', 'coin_creator_fee', 'cashback', 'buyback_fee', 'lp_fee_bps', 'protocol_fee_bps', 'coin_creator_fee_bps',
    'cu_price', 'cu_limit', 'signers_count', 'tips'];   // coin_creator — один на монету, в lp_pump_amm_pools
const POOL_COLUMNS = ['pool', 'mint', 'quote_mint', 'creator', 'coin_creator', 'pool_index', 'base_decimals', 'quote_decimals',
    'base_amount_in', 'quote_amount_in', 'lp_mint', 'is_mayhem', 'created_at_ms', 'slot', 'signature'];

function parseBuy(body) {
    const r = new Reader(body);
    const e = {is_buy: 1, event_ts: r.u64(), base_amount: r.u64(), limit_quote_amount: r.u64()};
    r.u64(); r.u64();   // резервы пользователя
    e.pool_base_reserves = r.u64(); e.pool_quote_reserves = r.u64(); e.quote_amount = r.u64();
    e.lp_fee_bps = r.u64(); e.lp_fee = r.u64(); e.protocol_fee_bps = r.u64(); e.protocol_fee = r.u64();
    r.u64();            // quote_amount_in_with_lp_fee
    e.user_quote_amount = r.u64(); e.pool = r.pubkey(); e.user = r.pubkey();
    r.o += 32 * 4;      // счета пользователя, получатель комиссии протокола и его счёт
    e.coin_creator = r.pubkey(); e.coin_creator_fee_bps = r.u64(); e.coin_creator_fee = r.u64();
    try {
        r.bool(); r.u64(); r.u64(); r.u64(); r.u64(); r.u64();   // track_volume, награды за объём, min_base_amount_out
        e.ix_name = r.string(); r.u64(); e.cashback = r.u64(); r.u64(); e.buyback_fee = r.u64();
    } catch (err) { /* старый формат события без хвоста */ }
    /** buy_exact_quote_in / buy_exact_quote_in_v2 / multi_hop_swap: программа пишет quote_amount_in = вся сумма с кошелька,
     *  user_quote_amount_in = сумма без комиссий — наоборот относительно buy (сверено по списанию с кошелька, 09.10).
     *  Правило по числам, не по имени инструкции: если quote_amount = user_quote_amount + lp + протокол + создатель — поля перевёрнуты.
     *  Итог для всех покупок: quote_amount — без комиссий, user_quote_amount — всего с кошелька. */
    const fees = BigInt(e.lp_fee) + BigInt(e.protocol_fee) + BigInt(e.coin_creator_fee);
    const d = BigInt(e.quote_amount) - BigInt(e.user_quote_amount) - fees;
    if (BigInt(e.quote_amount) > BigInt(e.user_quote_amount) && d >= -3n && d <= 3n) [e.quote_amount, e.user_quote_amount] = [e.user_quote_amount, e.quote_amount];
    return e;
}

function parseSell(body) {
    const r = new Reader(body);
    const e = {is_buy: 0, ix_name: 'sell', event_ts: r.u64(), base_amount: r.u64(), limit_quote_amount: r.u64()};
    r.u64(); r.u64();
    e.pool_base_reserves = r.u64(); e.pool_quote_reserves = r.u64(); e.quote_amount = r.u64();
    e.lp_fee_bps = r.u64(); e.lp_fee = r.u64(); e.protocol_fee_bps = r.u64(); e.protocol_fee = r.u64();
    r.u64();            // quote_amount_out_without_lp_fee
    e.user_quote_amount = r.u64(); e.pool = r.pubkey(); e.user = r.pubkey();
    r.o += 32 * 4;
    e.coin_creator = r.pubkey(); e.coin_creator_fee_bps = r.u64(); e.coin_creator_fee = r.u64();
    try { r.u64(); e.cashback = r.u64(); r.u64(); e.buyback_fee = r.u64(); } catch (err) {}
    return e;
}

function parseCreatePool(body) {
    const r = new Reader(body);
    r.u64();   // timestamp
    const p = {pool_index: r.b.readUInt16LE(r.o)}; r.o += 2;
    p.creator = r.pubkey(); p.mint = r.pubkey(); p.quote_mint = r.pubkey(); p.base_decimals = r.u8(); p.quote_decimals = r.u8();
    p.base_amount_in = r.u64(); p.quote_amount_in = r.u64();
    r.u64(); r.u64(); r.u64(); r.u64(); r.u64();   // pool_*_amount, minimum/initial liquidity, lp_token_amount_out
    r.u8();    // pool_bump
    p.pool = r.pubkey(); p.lp_mint = r.pubkey();
    r.o += 64; // счета пользователя
    p.coin_creator = r.pubkey();
    try { p.is_mayhem = r.bool() ? 1 : 0; } catch (err) { p.is_mayhem = null; }
    return p;
}

/**
 * Транзакция → {swaps, pools}. pools — только пулы миграции с кривой pump.fun; swaps — сделки в пулах, для которых poolInfo(pool)
 * возвращает {mint, quote_mint} (активное окно), остальные пропускаются.
 */
function decode(tx, meta, poolInfo) {
    const ixs = allInstructions(tx, meta.keys);
    const swaps = [], pools = [];
    const migration = meta.keys.includes(BONDING_CURVE);
    /** ev_index — номер события программы в транзакции (одинаков в потоке и в RPC): (signature, ev_index) — уникальный ключ сделки в MySQL */
    for (const [evIndex, ev] of anchorEvents(ixs, PROGRAM).entries()) {
        if (ev.disc === EV_CREATE_POOL) {
            if (!migration) continue;
            pools.push({...parseCreatePool(ev.body), created_at_ms: meta.ts, slot: meta.slot, signature: meta.signature});
        } else if (ev.disc === EV_BUY || ev.disc === EV_SELL) {
            const s = ev.disc === EV_BUY ? parseBuy(ev.body) : parseSell(ev.body);
            const info = poolInfo(s.pool);
            if (!info) continue;
            swaps.push({...s, mint: info.mint, quote_mint: info.quote_mint, signature: meta.signature, ev_index: evIndex, slot: meta.slot, ts: meta.ts,
                cu_price: meta.cuPrice, cu_limit: meta.cuLimit, signers_count: meta.signers, tips: meta.tips});
        }
    }
    return {swaps, pools};
}

module.exports = {name: 'pump_amm', PROGRAM, BONDING_CURVE, decode, parseBuy, parseSell, SWAP_COLUMNS, POOL_COLUMNS};
