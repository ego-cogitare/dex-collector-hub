/**
 * Raydium LaunchLab (LanMV9…): бондинг-кривые, на которых работают bonk.fun (LetsBonk) и другие площадки —
 * площадку отличает platform_config из аккаунтов инструкции. IDL — из блокчейна (версия 0.2.0, 07.10.2026).
 *
 * Сделка: событие TradeEvent (emit_cpi) — резервы (виртуальные и реальные до/после), суммы, комиссии, направление, статус пула;
 * монету, валюту, пользователя (payer) и platform_config берём из аккаунтов buy_/sell_exact_in/out с тем же pool_state.
 * Пул: PoolCreateEvent — создатель, конфиг, имя/символ/uri, параметры кривой; монета, валюта и platform_config — из initialize*.
 * Цена кривой (валюта за токен, сырые единицы): (virtual_quote + real_quote_after) / (virtual_base − real_base_after).
 */
const {allInstructions, anchorEvents, Reader} = require('../common');

const PROGRAM = 'LanMV9sAd7wArD4vJFi2qDdfnVhFxYSUg6eADduJ3uj';
const hex = a => Buffer.from(a).toString('hex');
const IX = {
    [hex([250, 234, 13, 123, 213, 156, 19, 236])]: 'buy_exact_in', [hex([24, 211, 116, 40, 105, 3, 153, 56])]: 'buy_exact_out',
    [hex([149, 39, 222, 155, 211, 124, 152, 26])]: 'sell_exact_in', [hex([95, 200, 71, 34, 8, 9, 11, 166])]: 'sell_exact_out',
    [hex([175, 175, 109, 31, 13, 152, 155, 237])]: 'initialize', [hex([67, 153, 175, 39, 218, 16, 38, 32])]: 'initialize_v2',
    [hex([37, 190, 126, 222, 44, 154, 171, 17])]: 'initialize_with_token_2022',
};
const EV_TRADE = hex([189, 219, 127, 211, 78, 230, 97, 238]), EV_POOL = hex([151, 215, 226, 9, 118, 161, 115, 174]);

const SWAP_COLUMNS = ['signature', 'ev_index', 'slot', 'ts', 'pool', 'mint', 'quote_mint', 'user', 'platform_config', 'global_config', 'is_buy', 'exact_in', 'ix',
    'amount_in', 'amount_out', 'virtual_base', 'virtual_quote', 'real_base_before', 'real_quote_before', 'real_base_after', 'real_quote_after',
    'total_base_sell', 'protocol_fee', 'platform_fee', 'creator_fee', 'share_fee', 'pool_status', 'cu_price', 'cu_limit', 'signers_count', 'tips'];
const POOL_COLUMNS = ['pool', 'mint', 'quote_mint', 'creator', 'payer', 'config', 'platform_config', 'decimals', 'name', 'symbol', 'uri',
    'curve_type', 'supply', 'total_base_sell', 'total_quote_fund_raising', 'migrate_type', 'vesting_locked', 'vesting_cliff', 'vesting_unlock',
    'amm_fee_on', 'created_at_ms', 'slot', 'signature'];

function parseTrade(body) {
    const r = new Reader(body);
    return {pool: r.pubkey(), total_base_sell: r.u64(), virtual_base: r.u64(), virtual_quote: r.u64(), real_base_before: r.u64(), real_quote_before: r.u64(),
        real_base_after: r.u64(), real_quote_after: r.u64(), amount_in: r.u64(), amount_out: r.u64(), protocol_fee: r.u64(), platform_fee: r.u64(),
        creator_fee: r.u64(), share_fee: r.u64(), is_buy: r.u8() === 0 ? 1 : 0, pool_status: r.u8(), exact_in: r.bool() ? 1 : 0};
}

function parsePoolCreate(body) {
    const r = new Reader(body);
    const ev = {pool: r.pubkey(), creator: r.pubkey(), config: r.pubkey(), decimals: r.u8(), name: r.string(), symbol: r.string(), uri: r.string()};
    const curve = r.u8();
    ev.curve_type = ['constant', 'fixed', 'linear'][curve] || String(curve);
    ev.supply = r.u64();
    if (curve === 0) ev.total_base_sell = r.u64();
    ev.total_quote_fund_raising = r.u64();
    ev.migrate_type = r.u8();
    ev.vesting_locked = r.u64(); ev.vesting_cliff = r.u64(); ev.vesting_unlock = r.u64();
    ev.amm_fee_on = r.o < body.length ? r.u8() : null;
    return ev;
}

/** Транзакция → {swaps, pools} (строки для MySQL) */
function decode(tx, meta) {
    const ixs = allInstructions(tx, meta.keys);
    /** инструкции программы: по pool_state — очередь аккаунтов сделок (в порядке исполнения) и данные инициализации */
    const trades = new Map(), inits = new Map();
    for (const x of ixs) {
        if (x.program !== PROGRAM || x.data.length < 8) continue;
        const name = IX[x.data.subarray(0, 8).toString('hex')];
        if (!name) continue;
        const a = x.accounts;
        if (name.startsWith('initialize')) inits.set(a[5], {payer: a[0], creator: a[1], global_config: a[2], platform_config: a[3], mint: a[6], quote_mint: a[7]});
        else { if (!trades.has(a[4])) trades.set(a[4], []); trades.get(a[4]).push({ix: name, user: a[0], global_config: a[2], platform_config: a[3], mint: a[9], quote_mint: a[10]}); }
    }
    const swaps = [], pools = [];
    /** ev_index — номер события программы в транзакции: (signature, ev_index) — уникальный ключ сделки в MySQL */
    for (const [evIndex, ev] of anchorEvents(ixs, PROGRAM).entries()) {
        if (ev.disc === EV_TRADE) {
            const t = parseTrade(ev.body), q = trades.get(t.pool), acc = q && q.length ? q.shift() : {};
            swaps.push({...t, ...acc, signature: meta.signature, ev_index: evIndex, slot: meta.slot, ts: meta.ts, cu_price: meta.cuPrice, cu_limit: meta.cuLimit, signers_count: meta.signers, tips: meta.tips});
        } else if (ev.disc === EV_POOL) {
            const p = parsePoolCreate(ev.body), init = inits.get(p.pool) || {};
            pools.push({...p, mint: init.mint, quote_mint: init.quote_mint, payer: init.payer, platform_config: init.platform_config,
                created_at_ms: meta.ts, slot: meta.slot, signature: meta.signature});
        }
    }
    return {swaps, pools};
}

module.exports = {name: 'launchlab', PROGRAM, decode, SWAP_COLUMNS, POOL_COLUMNS};
