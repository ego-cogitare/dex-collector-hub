/**
 * Meteora Dynamic Bonding Curve (dbcij3…): движок кривых, на котором запускают монеты Bags, Believe, Jupiter Studio и др. —
 * площадку отличает config (у каждой площадки свои). IDL — из блокчейна (версия 0.1.10, 07.10.2026).
 *
 * Монеты с transfer hook (Token-2022) торгуются инструкцией swap2_with_transfer_hook и выпускают EvtSwap2WithTransferHook (поля как у EvtSwap2),
 * создаются initialize_virtual_pool_with_token2022_transfer_hook → EvtInitializePoolWithTransferHook (поля как у EvtInitializePool); в IDL из блокчейна их нет.
 * Сделка: события EvtSwap / EvtSwap2 (emit_cpi; на одну сделку программа выпускает оба — берём EvtSwap2, старое — только если нового нет) — суммы, комиссии, цена после сделки (next_sqrt_price, Q64.64),
 * у EvtSwap2 ещё резерв валюты и порог миграции; монету, валюту и пользователя (payer) берём из аккаунтов swap/swap2 того же пула.
 * Направление: trade_direction 0 — продажа монеты (base → quote), 1 — покупка (quote → base).
 * Цена (валюта за токен, сырые единицы): (next_sqrt_price / 2^64)^2.
 * Пул: EvtInitializePool — config, создатель, монета, тип пула; валюта — из initialize_virtual_pool_*.
 */
const {allInstructions, anchorEvents, Reader} = require('../common');

const PROGRAM = 'dbcij3LWUppWqq96dh6gJWwBifmcGfLSB5D4DuSMaqN';
const hex = a => Buffer.from(a).toString('hex');
const IX = {
    [hex([248, 198, 158, 145, 225, 117, 135, 200])]: 'swap', [hex([65, 75, 63, 76, 235, 91, 91, 136])]: 'swap2',
    [hex([140, 85, 215, 176, 102, 54, 104, 79])]: 'init_spl', [hex([169, 118, 51, 78, 145, 110, 220, 155])]: 'init_2022',
    /** нет в IDL из блокчейна (0.1.10), видны в потоке (07.10): дискриминаторы = sha256('global:<имя>'), аккаунты в том же порядке, что у swap2 / init */
    b75d992818e6c297: 'swap2_with_transfer_hook', b60de9b12a918702: 'init_2022_transfer_hook',
};
const EV_SWAP = hex([27, 60, 21, 213, 138, 170, 187, 147]), EV_SWAP2 = hex([189, 66, 51, 168, 38, 80, 117, 153]);
const EV_INIT = hex([228, 50, 246, 85, 203, 66, 134, 37]), EV_COMPLETE = hex([229, 231, 86, 84, 156, 134, 75, 24]);
/** события монет с transfer hook — те же поля, что у EvtSwap2 / EvtInitializePool (sha256('event:<имя>')) */
const EV_SWAP2_HOOK = '863ba8785e3372e7', EV_INIT_HOOK = 'd589a435c14a0f6e';

const SWAP_COLUMNS = ['signature', 'ev_index', 'slot', 'ts', 'pool', 'config', 'mint', 'quote_mint', 'user', 'is_buy', 'has_referral', 'ev', 'swap_mode',
    'amount_in', 'amount_out', 'param_0', 'param_1', 'excluded_fee_input', 'amount_left', 'next_sqrt_price', 'trading_fee', 'protocol_fee', 'referral_fee',
    'quote_reserve', 'migration_threshold', 'event_ts', 'cu_price', 'cu_limit', 'signers_count', 'tips'];
const POOL_COLUMNS = ['pool', 'config', 'creator', 'mint', 'quote_mint', 'payer', 'pool_type', 'activation_point', 'created_at_ms', 'slot', 'signature'];
const COMPLETE_COLUMNS = ['pool', 'config', 'base_reserve', 'quote_reserve', 'ts', 'slot', 'signature'];

function parseSwap(body, v2) {
    const r = new Reader(body);
    const e = {pool: r.pubkey(), config: r.pubkey(), is_buy: r.u8() === 1 ? 1 : 0, has_referral: r.bool() ? 1 : 0, ev: v2 ? 2 : 1};
    if (v2) {
        e.param_0 = r.u64(); e.param_1 = r.u64(); e.swap_mode = r.u8();
        e.amount_in = r.u64(); e.excluded_fee_input = r.u64(); e.amount_left = r.u64(); e.amount_out = r.u64();
        e.next_sqrt_price = r.u128(); e.trading_fee = r.u64(); e.protocol_fee = r.u64(); e.referral_fee = r.u64();
        e.quote_reserve = r.u64(); e.migration_threshold = r.u64(); e.event_ts = r.u64();
    } else {
        e.param_0 = r.u64(); e.param_1 = r.u64();
        const actualIn = r.u64(); e.amount_out = r.u64(); e.next_sqrt_price = r.u128(); e.trading_fee = r.u64(); e.protocol_fee = r.u64(); e.referral_fee = r.u64();
        e.amount_in = r.u64(); e.excluded_fee_input = actualIn; e.event_ts = r.u64();
    }
    return e;
}

/** Транзакция → {swaps, pools, completes} */
function decode(tx, meta) {
    const ixs = allInstructions(tx, meta.keys);
    const swapsAcc = new Map(), inits = new Map();
    for (const x of ixs) {
        if (x.program !== PROGRAM || x.data.length < 8) continue;
        const name = IX[x.data.subarray(0, 8).toString('hex')];
        if (!name) continue;
        const a = x.accounts;
        if (name.startsWith('swap')) { if (!swapsAcc.has(a[2])) swapsAcc.set(a[2], []); swapsAcc.get(a[2]).push({mint: a[7], quote_mint: a[8], user: a[9]}); }
        else inits.set(a[5], {quote_mint: a[4], payer: name === 'init_spl' ? a[10] : name === 'init_2022' ? a[8] : a[9]});
    }
    const swaps = [], pools = [], completes = [];
    const events = anchorEvents(ixs, PROGRAM);
    /** программа на каждую сделку выпускает и старое EvtSwap, и новое EvtSwap2 — если есть EvtSwap2, берём только их */
    const hasV2 = events.some(e => e.disc === EV_SWAP2 || e.disc === EV_SWAP2_HOOK);
    /** ev_index — номер события программы в транзакции: (signature, ev_index) — уникальный ключ сделки в MySQL */
    for (const [evIndex, ev] of events.entries()) {
        if (ev.disc === EV_SWAP && hasV2) continue;
        if (ev.disc === EV_SWAP || ev.disc === EV_SWAP2 || ev.disc === EV_SWAP2_HOOK) {
            const s = parseSwap(ev.body, ev.disc !== EV_SWAP), q = swapsAcc.get(s.pool), acc = q && q.length ? q.shift() : {};
            swaps.push({...s, ...acc, signature: meta.signature, ev_index: evIndex, slot: meta.slot, ts: meta.ts, cu_price: meta.cuPrice, cu_limit: meta.cuLimit, signers_count: meta.signers, tips: meta.tips});
        } else if (ev.disc === EV_INIT || ev.disc === EV_INIT_HOOK) {
            const r = new Reader(ev.body);
            const p = {pool: r.pubkey(), config: r.pubkey(), creator: r.pubkey(), mint: r.pubkey(), pool_type: r.u8(), activation_point: r.u64()};
            const init = inits.get(p.pool) || {};
            pools.push({...p, quote_mint: init.quote_mint, payer: init.payer, created_at_ms: meta.ts, slot: meta.slot, signature: meta.signature});
        } else if (ev.disc === EV_COMPLETE) {
            const r = new Reader(ev.body);
            completes.push({pool: r.pubkey(), config: r.pubkey(), base_reserve: r.u64(), quote_reserve: r.u64(), ts: meta.ts, slot: meta.slot, signature: meta.signature});
        }
    }
    return {swaps, pools, completes};
}

module.exports = {name: 'meteora_dbc', PROGRAM, decode, SWAP_COLUMNS, POOL_COLUMNS, COMPLETE_COLUMNS};
