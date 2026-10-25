/**
 * Фильтры подписки клиента. Клиент в приветствии передаёт subscribe — по пулам (площадкам) набор условий:
 *
 *   {"pump":        {"bc": true, "amm": true, "mayhem": false, "quotes": ["SOL", "pumpCm…"], "creatorChange": true, "pools": true},
 *    "launchlab":   {"quotes": [...], "platforms": [...], "pools": true},
 *    "meteora_dbc": {"quotes": [...], "configs": [...], "pools": true, "completes": true},
 *    "blockhash": true,
 *    "bsc": {"fourMeme": true, "flap": true, "tokens": true},
 *    "meta": true, "farm": true}
 *
 *  pump.bc / pump.amm — кривая / PumpSwap (по умолчанию true, если ключ pump есть);
 *  mayhem — true: включать mayhem-монеты (по умолчанию), false — исключать, "only" — только их;
 *  quotes — валюты котировки (контракты; SOL — "SOL" или WSOL); нет ключа — все;
 *  creatorChange — смена криейтора кривой (по умолчанию true при bc); pools / completes — новые пулы / завершения кривой (по умолчанию true);
 *  launchlab.platforms — platform_config; meteora_dbc.configs — config пула; blockhash — свежий blockhash (confirmed) при каждой смене;
 *  bsc — Four.meme / Flap.sh (BNB Chain): сделки кривой и новые токены (tokens), отмена реорганизацией — запись {removed, tx_hash, log_index};
 *  meta — метадата новых монет pump.fun (JSON по uri, src/sources/pump-meta.js); farm — состояние ферм операторов (снимок при изменении, новому подписчику — последний).
 * Совместимость: старое "channels": [...] — каналы целиком, без условий.
 *
 * compile(subscribe) → Map(канал → функция(запись) → запись | null): для pump_bc сделки фильтруются поштучно
 * (если прошли не все — клиенту уходит копия записи с подходящими сделками).
 */
const WSOL = 'So11111111111111111111111111111111111111112';
const normQuote = q => (q === 'SOL' || q === WSOL || q === null || q === undefined ? 'SOL' : q);

function quoteOk(set, q) { return !set || set.has(normQuote(q)); }
function mayhemOk(mode, v) { if (mode === undefined || mode === true) return true; if (mode === 'only') return !!v; return !v; }

function compile(sub) {
    const out = new Map();
    const pump = sub.pump;
    if (pump) {
        const quotes = pump.quotes ? new Set(pump.quotes.map(normQuote)) : null;
        if (pump.bc !== false) {
            const cc = pump.creatorChange !== false;
            out.set('pump_bc', r => {
                if (r.creatorChange) return cc ? r : null;
                const ok = r.swaps.filter(s => mayhemOk(pump.mayhem, s.mayhem_mode) && quoteOk(quotes, s.quote_mint));
                if (!ok.length) return null;
                return ok.length === r.swaps.length ? r : {...r, swaps: ok, _partial: true};
            });
        }
        if (pump.amm !== false) {
            const f = r => mayhemOk(pump.mayhem, r.is_mayhem) && quoteOk(quotes, r.quote_mint) ? r : null;
            out.set('pump_amm', f);
            if (pump.pools !== false) out.set('pump_amm_pool', f);
        }
    }
    for (const [name, key] of [['launchlab', 'platforms'], ['meteora_dbc', 'configs']]) {
        const s = sub[name]; if (!s) continue;
        const quotes = s.quotes ? new Set(s.quotes.map(normQuote)) : null, list = s[key] ? new Set(s[key]) : null;
        const field = name === 'launchlab' ? 'platform_config' : 'config';
        const f = r => quoteOk(quotes, r.quote_mint) && (!list || list.has(r[field])) ? r : null;
        out.set(name, f);
        if (s.pools !== false) out.set(`${name}_pool`, f);
        if (name === 'meteora_dbc' && s.completes !== false) out.set('meteora_dbc_complete', r => (!list || list.has(r.config) ? r : null));
    }
    if (sub.blockhash) out.set('blockhash', r => r);
    if (sub.meta) out.set('meta', r => r);
    if (sub.farm) out.set('farm', r => r);
    if (sub.bsc) {
        const all = r => r;
        if (sub.bsc.fourMeme !== false) { out.set('bsc_four_meme', all); if (sub.bsc.tokens !== false) out.set('bsc_four_meme_token', all); }
        if (sub.bsc.flap !== false) { out.set('bsc_flap', all); if (sub.bsc.tokens !== false) out.set('bsc_flap_token', all); }
    }
    return out;
}

module.exports = {compile, normQuote};
