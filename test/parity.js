/** Сверка записи хаба (база HUB_DB, по умолчанию hub_test) с основной базой (MYSQL_DATABASE) за окно [FROM, TO] (ISO UTC):
 *  pump_fun_bc_mints и pump_fun_bc_swaps — монеты, созданные в [FROM, TO − 4 мин]; AMM — пулы, созданные в окне, и их сделки;
 *  LaunchLab / DBC — сделки в окне по (signature, ev_index). node test/parity.js 2026-10-10T11:21:00Z 2026-10-10T11:36:00Z */
require('dotenv').config({path: require('path').join(__dirname, '../.env'), quiet: true});
const mysql = require('mysql2/promise');
const [FROM, TO] = process.argv.slice(2).map(s => Date.parse(s)); const HUB = process.env.HUB_DB || 'hub_test', MAIN = process.env.MAIN_DB || process.env.MYSQL_DATABASE;
const cmp = (name, A, B, keyF, fields) => {
  const a = new Map(A.map(r => [keyF(r), r])), b = new Map(B.map(r => [keyF(r), r]));
  const onlyA = [...a.keys()].filter(k => !b.has(k)), onlyB = [...b.keys()].filter(k => !a.has(k));
  const diff = {}; let both = 0;
  for (const [k, r] of a) { const h = b.get(k); if (!h) continue; both++; for (const f of fields) if (String(r[f]) !== String(h[f])) { diff[f] = (diff[f] || 0) + 1; if (!diff['_ex_' + f]) diff['_ex_' + f] = [String(r[f]).slice(0, 80), String(h[f]).slice(0, 80)]; } }
  console.log(`${name}: основная ${a.size}, хаб ${b.size}, общих ${both}, только в основной ${onlyA.length}, только в хабе ${onlyB.length}` + (Object.keys(diff).length ? ' | расхождения полей: ' + JSON.stringify(diff) : ' | поля совпадают'));
  if (onlyA.length) console.log('   пример только в основной:', onlyA.slice(0, 3)); if (onlyB.length) console.log('   пример только в хабе:', onlyB.slice(0, 3));
};
(async () => {
  const c = await mysql.createConnection({host: process.env.MYSQL_HOST, user: process.env.MYSQL_USERNAME, password: process.env.MYSQL_PASSWORD});
  const q = async (db, sql, p) => (await c.query(sql.replace(/\bDB\./g, db + '.'), p))[0];
  const mTo = TO - 240000;
  for (const [name, sql] of [['pump_fun_bc_mints', 'SELECT * FROM DB.pump_fun_bc_mints WHERE created_at_ms BETWEEN ? AND ?']]) {
    const A = await q(MAIN, sql, [FROM, mTo]), B = await q(HUB, sql, [FROM, mTo]);
    cmp(name, A, B, r => r.mint, ['creator', 'mayhem_mode', 'quote_mint', 'created_at_ms']);
    const mints = A.map(r => r.mint).concat(B.map(r => r.mint));
    if (mints.length) {
      const s = 'SELECT signature, mint, user, block, amount_sol, price_sol, vsr, vtr, amount_token, timestamp, cu_price, cu_limit, protocol_fee, creator_fee, signers_count, tips FROM DB.pump_fun_bc_swaps WHERE mint IN (?)';
      cmp('pump_fun_bc_swaps (монеты окна)', await q(MAIN, s, [mints]), await q(HUB, s, [mints]), r => `${r.signature}|${r.mint}|${r.user}|${r.amount_sol}|${r.vsr}`,
        ['block', 'price_sol', 'vtr', 'amount_token', 'timestamp', 'cu_price', 'cu_limit', 'protocol_fee', 'creator_fee', 'signers_count', 'tips']);
    }
  }
  const ammP = 'SELECT * FROM DB.lp_pump_amm_pools WHERE created_at_ms BETWEEN ? AND ?';
  const PA = await q(MAIN, ammP, [FROM, mTo]), PB = await q(HUB, ammP, [FROM, mTo]);
  cmp('lp_pump_amm_pools', PA, PB, r => r.pool, ['mint', 'quote_mint', 'coin_creator', 'is_mayhem', 'slot', 'signature']);
  const pools = PA.map(r => r.pool).concat(PB.map(r => r.pool));
  if (pools.length) { const s = 'SELECT * FROM DB.lp_pump_amm_swaps WHERE pool IN (?) AND ts <= ?'; cmp('lp_pump_amm_swaps (пулы окна)', await q(MAIN, s, [pools, TO]), await q(HUB, s, [pools, TO]), r => `${r.signature}|${r.ev_index}`, ['pool', 'mint', 'user', 'base_amount', 'quote_amount', 'user_quote_amount', 'pool_base_reserves', 'pool_quote_reserves', 'slot']); }
  for (const t of ['lp_launchlab_swaps', 'lp_meteora_dbc_swaps']) {
    const s = `SELECT * FROM DB.${t} WHERE ts BETWEEN ? AND ?`;
    const A = await q(MAIN, s, [FROM, TO]), B = await q(HUB, s, [FROM, TO]);
    const cols = Object.keys(A[0] || B[0] || {}).filter(k => !['id', 'ts', 'cu_price', 'cu_limit', 'tips', 'signers_count'].includes(k));
    cmp(t, A, B, r => `${r.signature}|${r.ev_index}`, cols);
  }
  await c.end();
})().catch(e => { console.error(e.message); process.exit(1); });
