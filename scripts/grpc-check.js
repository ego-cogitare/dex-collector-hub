/**
 * Проверка узла Yellowstone gRPC (например Chainstack) против Triton: поток транзакций PumpSwap AMM на SECONDS секунд с каждого узла параллельно —
 * сообщений, задержка createdAt → приход (p50/p90/p99), сколько подписей пришло раньше с одного узла, чем с другого; затем — сколько потоков узел даёт держать одновременно (до 4).
 *   AMM_GRPC_URL=… AMM_GRPC_TOKEN=… node scripts/grpc-check.js [SECONDS=60]   (Triton — из WS_RPC / TRITON_ONE_TOKEN в .env)
 */
require('dotenv').config({quiet: true});
const {default: Client, CommitmentLevel} = require('@triton-one/yellowstone-grpc');
const bs58 = require('bs58').default;
const SECONDS = +(process.argv[2] || 60), AMM = 'pAMMBay6oceH9fJKBRHGP5D4bD4sWpmSwMn52FMfXEA';
const nodes = {triton: [process.env.WS_RPC, process.env.TRITON_ONE_TOKEN], other: [process.env.AMM_GRPC_URL, process.env.AMM_GRPC_TOKEN]};
const req = {accounts: {}, slots: {}, transactionsStatus: {}, blocks: {}, blocksMeta: {}, entry: {}, accountsDataSlice: [],
    transactions: {amm: {accountInclude: [AMM], accountExclude: [], accountRequired: [], vote: false, failed: false}},
    commitment: CommitmentLevel.PROCESSED, includeTransactions: true, includeAccounts: false, includeEntries: false};
async function open([url, token]) { const c = new Client(url, token, {'grpc.keepalive_time_ms': 20000}); await c.connect(); const s = await c.subscribe(); await new Promise((r, j) => s.write(req, e => e ? j(e) : r())); return s; }
const q = (a, p) => a.length ? a[Math.min(a.length - 1, Math.floor(p * a.length))] : null;
(async () => {
    if (!nodes.other[0]) throw new Error('нет AMM_GRPC_URL');
    const seen = {}, lag = {triton: [], other: []}, n = {triton: 0, other: 0}, first = {triton: 0, other: 0}, streams = [], diff = [];
    for (const k of Object.keys(nodes)) {
        const s = await open(nodes[k]); streams.push(s);
        s.on('data', d => { if (!d.transaction) return; const now = Date.now(); n[k]++;
            if (d.createdAt) lag[k].push(now - new Date(d.createdAt).getTime());
            const sig = bs58.encode(d.transaction.transaction.signature); const o = seen[sig];
            if (!o) seen[sig] = {k, t: now}; else if (o.k !== k) { first[o.k]++; diff.push(o.k === 'triton' ? now - o.t : o.t - now); delete seen[sig]; } });
        s.on('error', e => console.log(k, 'ошибка потока:', e.message));
    }
    await new Promise(r => setTimeout(r, SECONDS * 1000));
    for (const k of Object.keys(nodes)) { const a = lag[k].sort((x, y) => x - y);
        console.log(`${k}: сообщений ${n[k]} (${(n[k] / SECONDS).toFixed(0)}/с), createdAt → приход p50 ${q(a, .5)} p90 ${q(a, .9)} p99 ${q(a, .99)} мс${a.length ? '' : ' (createdAt нет)'}, пришло первым ${first[k]}`); }
    const d = diff.sort((x, y) => x - y), only = {triton: 0, other: 0}; for (const v of Object.values(seen)) only[v.k]++;
    console.log(`одна и та же транзакция: приход с узла минус приход с Triton (мс, > 0 — Triton раньше), общих ${d.length}: p10 ${q(d, .1)} p25 ${q(d, .25)} p50 ${q(d, .5)} p75 ${q(d, .75)} p90 ${q(d, .9)} p99 ${q(d, .99)}`);
    console.log(`только с Triton ${only.triton}, только с узла ${only.other} (последние секунды окна — частично в пути)`);
    streams.forEach(s => { try { s.destroy(); } catch (e) {} });
    // сколько потоков одновременно
    const held = [];
    for (let i = 1; i <= 4; i++) {
        try { const s = await open(nodes.other); let fail = null; s.on('error', e => { fail = e.message; }); held.push(s);
            await new Promise(r => setTimeout(r, 4000)); console.log(`поток ${i}: ${fail ? 'отказ — ' + fail : 'ок'}`); if (fail) break; }
        catch (e) { console.log(`поток ${i}: отказ — ${e.message}`); break; }
    }
    held.forEach(s => { try { s.destroy(); } catch (e) {} });
    process.exit(0);
})().catch(e => { console.error(e.message); process.exit(1); });
