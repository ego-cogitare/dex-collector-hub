/**
 * Дозаполнение lp_pump_amm_pools.virtual_quote_reserves (24.10): пулы с NULL — аккаунты пачками по 100 (getMultipleAccounts), u64 по смещению 245
 * (как src/sources/pump-amm-runner.js: «сторожевое» > 1e17 — 0; аккаунта нет / короче — остаётся NULL). RPC — AMM_HTTP_RPC || RPC, не чаще RPS (по умолчанию 3) запросов в секунду.
 *   node scripts/amm-virtual-backfill.js [--dry-run]
 */
require('dotenv').config({quiet: true});
const mysql = require('mysql2/promise');
const URL = process.env.AMM_HTTP_RPC || process.env.RPC, RPS = Number(process.env.RPS || 3), DRY = process.argv.includes('--dry-run');
const OFF = 245, SENT = 10n ** 17n;
const sleep = ms => new Promise(r => setTimeout(r, ms));
async function rpc(method, params) {
    for (let i = 0; i < 6; i++) {
        try {
            const r = await fetch(URL, {method: 'POST', headers: {'content-type': 'application/json'}, body: JSON.stringify({jsonrpc: '2.0', id: 1, method, params}), signal: AbortSignal.timeout(30000)});
            const j = await r.json(); if (j.error) throw new Error(JSON.stringify(j.error).slice(0, 200)); return j.result;
        } catch (e) { if (i === 5) throw e; await sleep(1000 * (i + 1)); }
    }
}
(async () => {
    const db = await mysql.createConnection({host: process.env.MYSQL_HOST, user: process.env.MYSQL_USERNAME, password: process.env.MYSQL_PASSWORD, database: process.env.MYSQL_DATABASE});
    const [rows] = await db.query('SELECT pool FROM lp_pump_amm_pools WHERE virtual_quote_reserves IS NULL');
    console.log('пулов без добавки', rows.length);
    let ok = 0, zero = 0, none = 0;
    for (let i = 0; i < rows.length; i += 100) {
        const keys = rows.slice(i, i + 100).map(r => r.pool);
        const res = await rpc('getMultipleAccounts', [keys, {encoding: 'base64', commitment: 'confirmed'}]);
        const upd = [];
        res.value.forEach((v, k) => {
            if (!v) { none++; return; }
            const b = Buffer.from(v.data[0], 'base64');
            if (b.length < OFF + 8) { none++; return; }
            let x = b.readBigUInt64LE(OFF); if (x > SENT) x = 0n;
            if (x === 0n) zero++; else ok++;
            upd.push([keys[k], x.toString()]);
        });
        if (upd.length && !DRY) await db.query(`UPDATE lp_pump_amm_pools SET virtual_quote_reserves = CASE pool ${upd.map(() => 'WHEN ? THEN ?').join(' ')} END WHERE pool IN (?)`, [...upd.flat(), upd.map(u => u[0])]);
        await sleep(1000 / RPS);
    }
    console.log(`готово: добавка > 0 — ${ok}, 0 — ${zero}, без аккаунта / короче — ${none}${DRY ? ' (без записи)' : ''}`);
    await db.end();
})().catch(e => { console.error(e); process.exit(1); });
