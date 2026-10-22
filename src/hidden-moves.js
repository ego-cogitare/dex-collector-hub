/**
 * «Скрытые» движения SOL между кошельками — без системного перевода, поэтому сборщик переводов (tools/sol-transfers.js) их не видел (22.10, решение пользователя;
 * пример — монета 8qQzfLNs…: продавец в транзакции продажи создаёт WSOL-счёт с выручкой на чужой кошелёк, тот в том же слоте закрывает его и покупает; выручка — через stake).
 * kind (как колонка kind в CSV переводов):
 *   5 — WSOL-счёт подарен: создан (CreateAccount / CreateAccountWithSeed, владелец программы — токен-программа) или пополнен системным переводом кошельком-плательщиком,
 *       а владелец счёта (InitializeAccount* в транзакции или по балансам токенов) — другой кошелёк: плательщик → владелец;
 *   6 — перевод WSOL (Transfer / TransferChecked) между счетами разных владельцев: владелец-отправитель → владелец-получатель;
 *   7 — токен-счёт закрыт (CloseAccount) на чужой кошелёк: владелец → получатель (WSOL — весь остаток, прочие — аренда);
 *   8 — вывод из stake-аккаунта (Withdraw) на другой кошелёк: кто создал stake-аккаунт в этой транзакции (иначе — withdraw-authority) → получатель.
 * Берём только кошельки (точки на кривой ed25519): хранилища пулов и PDA программ не связывают. Сумма ≥ minLamports (0.01 SOL: аренда счёта 0.002 — шум сервисов, создающих и закрывающих счета за пользователей).
 * Копия файла copy-trade-solana/tools/hidden-moves.js (держать одинаковыми): хаб разбирает свои потоки площадок и шлёт связи сборщику переводов (src/hidden-sink.js, POST /edges).
 *   const {scan, fromGrpc, fromRpcJson} = require('./hidden-moves'); scan(fromGrpc(d)) → [{kind, from, to, lamports}]
 */
'use strict';
const bs58m = require('bs58'); const bs58 = bs58m.default || bs58m;

const SYSTEM = '11111111111111111111111111111111', TOKEN = 'TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA', TOKEN22 = 'TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb';
const WSOL = 'So11111111111111111111111111111111111111112', STAKE = 'Stake11111111111111111111111111111111111111';
const TOKENS = new Set([TOKEN, TOKEN22]);
/** программы и служебные адреса — не кошельки, хоть и на кривой */
const NOT_WALLET = new Set([SYSTEM, TOKEN, TOKEN22, STAKE, WSOL, 'ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL', 'ComputeBudget111111111111111111111111111111', 'SysvarRent111111111111111111111111111111111']);

/** точка ed25519 на кривой (кошелёк, а не PDA) — как CompressedEdwardsY::decompress */
const P = (1n << 255n) - 19n;
const modp = a => { const r = a % P; return r < 0n ? r + P : r; };
const pow = (b, e) => { let r = 1n; b = modp(b); while (e > 0n) { if (e & 1n) r = r * b % P; b = b * b % P; e >>= 1n; } return r; };
const D = modp(-121665n * pow(121666n, P - 2n));
const curveCache = new Map();
function onCurve(addr) {
    let v = curveCache.get(addr); if (v !== undefined) return v;
    v = false;
    try {
        const b = Buffer.from(bs58.decode(addr));
        if (b.length === 32) {
            let y = 0n; for (let i = 31; i >= 0; i--) y = (y << 8n) | BigInt(i === 31 ? b[i] & 0x7f : b[i]);
            if (y < P) {
                const y2 = y * y % P, u = modp(y2 - 1n), w = modp(D * y2 + 1n);
                const x = u * pow(w, 3n) % P * pow(u * pow(w, 7n), (P - 5n) / 8n) % P;
                const vx2 = w * x % P * x % P;
                v = vx2 === u || vx2 === modp(-u);   // корень есть (второй случай — x · sqrt(−1))
            }
        }
    } catch (e) { v = false; }
    if (curveCache.size > 500000) curveCache.clear();
    curveCache.set(addr, v); return v;
}

const u64 = (b, o) => b.length >= o + 8 ? b.readBigUInt64LE(o) : 0n;

/** n — нормализованная транзакция: {keys, ixs: [{program, accounts, data: Buffer}], tb: [{account, mint, owner}], pre, post, sig} */
function scan(n, {minLamports = 10_000_000} = {}) {
    const out = [], seenE = new Set();
    const own = new Map(); for (const t of n.tb) if (t.account && t.owner) own.set(t.account, {mint: t.mint, owner: t.owner});
    const created = new Map(), createdStake = new Map();
    const idx = new Map(n.keys.map((k, i) => [k, i]));
    for (const ix of n.ixs) {
        const d = ix.data;
        if (ix.program === SYSTEM && d.length >= 4) {
            const tag = d.readUInt32LE(0);
            if (tag === 0 && d.length >= 52) {                       // CreateAccount: lamports, space, owner
                const owner = bs58.encode(d.subarray(20, 52)), rec = {from: ix.accounts[0], lamports: u64(d, 4)};
                if (TOKENS.has(owner)) created.set(ix.accounts[1], rec); else if (owner === STAKE) createdStake.set(ix.accounts[1], rec);
            } else if (tag === 3 && d.length >= 44) {                // CreateAccountWithSeed: base, seed, lamports, space, owner
                const o = 44 + Number(u64(d, 36));
                if (d.length >= o + 48) {
                    const owner = bs58.encode(d.subarray(o + 16, o + 48)), rec = {from: ix.accounts[0], lamports: u64(d, o)};
                    if (TOKENS.has(owner)) created.set(ix.accounts[1], rec); else if (owner === STAKE) createdStake.set(ix.accounts[1], rec);
                }
            }
        } else if (TOKENS.has(ix.program) && d.length >= 1) {
            const t = d[0];
            if (t === 1 && ix.accounts.length >= 3) own.set(ix.accounts[0], {mint: ix.accounts[1], owner: ix.accounts[2]});                       // InitializeAccount
            else if ((t === 16 || t === 18) && d.length >= 33) own.set(ix.accounts[0], {mint: ix.accounts[1], owner: bs58.encode(d.subarray(1, 33))});   // InitializeAccount2 / 3
        }
    }
    const edge = (kind, from, to, lam) => {
        if (!from || !to || from === to || lam < BigInt(minLamports)) return;
        const k = `${kind}:${from}:${to}`; if (seenE.has(k)) return;
        if (NOT_WALLET.has(from) || NOT_WALLET.has(to) || !onCurve(from) || !onCurve(to)) return;
        seenE.add(k); out.push({kind, from, to, lamports: Number(lam)});
    };
    for (const [acc, c] of created) { const o = own.get(acc); if (o && o.mint === WSOL) edge(5, c.from, o.owner, c.lamports); }
    for (const ix of n.ixs) {
        const d = ix.data;
        if (ix.program === SYSTEM && d.length >= 12) {
            const tag = d.readUInt32LE(0);
            if (tag === 2 || tag === 11) {                           // Transfer / TransferWithSeed на чужой WSOL-счёт (пополнение + SyncNative)
                const from = ix.accounts[0], to = ix.accounts[tag === 2 ? 1 : 2], o = own.get(to);
                if (o && o.mint === WSOL && !created.has(to)) edge(5, from, o.owner, u64(d, 4));
            }
        } else if (TOKENS.has(ix.program) && d.length >= 9) {
            const t = d[0];
            if (t === 3 || t === 12) {                               // Transfer: src, dst, auth / TransferChecked: src, mint, dst, auth
                const src = ix.accounts[0], dst = ix.accounts[t === 3 ? 1 : 2], os = own.get(src), od = own.get(dst);
                const mint = t === 12 ? ix.accounts[1] : (os && os.mint) || (od && od.mint);
                if (mint === WSOL && os && od && os.owner !== od.owner) edge(6, os.owner, od.owner, u64(d, 1));
            }
        } else if (TOKENS.has(ix.program) && d.length >= 1 && d[0] === 9 && ix.accounts.length >= 3) {   // CloseAccount: account, destination, owner
            const acc = ix.accounts[0], dest = ix.accounts[1], o = own.get(acc), owner = (o && o.owner) || ix.accounts[2];
            const i = idx.get(acc); let lam = i !== undefined && n.pre[i] ? BigInt(n.pre[i]) : 0n;
            if (created.has(acc)) lam += created.get(acc).lamports;
            if (owner !== dest) edge(7, owner, dest, lam);
        } else if (ix.program === STAKE && d.length >= 12 && d.readUInt32LE(0) === 4 && ix.accounts.length >= 5) {   // Withdraw: stake, to, clock, history, authority
            const st = ix.accounts[0], to = ix.accounts[1], c = createdStake.get(st);
            edge(8, c ? c.from : ix.accounts[4], to, u64(d, 4));
        }
    }
    return out;
}

/** Быстрый предфильтр по сырой транзакции gRPC (без перевода ключей в base58): есть ли кандидат на связь — иначе полный разбор не нужен.
 *  Отсекает обычные свопы: WSOL-счёт создан / пополнен для себя, закрытие на себя, переводы WSOL в хранилища пулов (владелец — PDA, кэш onCurve). */
const B = a => Buffer.from(bs58.decode(a));
const SYSTEM_B = Buffer.alloc(32), TOKEN_B = B(TOKEN), TOKEN22_B = B(TOKEN22), STAKE_B = B(STAKE), WSOL_B = B(WSOL);
const eq = (a, b) => a && b && a.length === 32 && Buffer.compare(Buffer.from(a.buffer, a.byteOffset, 32), b) === 0;
const isTok = k => eq(k, TOKEN_B) || eq(k, TOKEN22_B);
function quick(d, minLamports = 10_000_000) {
    const tx = d.transaction.transaction, msg = tx.transaction.message, meta = tx.meta || {};
    const kb = [...msg.accountKeys, ...(meta.loadedWritableAddresses || []), ...(meta.loadedReadonlyAddresses || [])];
    const min = BigInt(minLamports);
    const all = [...msg.instructions]; for (const g of meta.innerInstructions || []) for (const x of g.instructions) all.push(x);
    let tbOwner = null;   // accountIndex → {mint, owner} (строки из меты)
    const tbo = () => { if (!tbOwner) { tbOwner = new Map(); for (const b of [...(meta.preTokenBalances || []), ...(meta.postTokenBalances || [])]) tbOwner.set(b.accountIndex, {mint: b.mint, owner: b.owner}); } return tbOwner; };
    const initOwner = new Map();   // индекс счёта → байты владельца (InitializeAccount*)
    for (const ix of all) {
        if (!isTok(kb[ix.programIdIndex])) continue;
        const dt = ix.data; if (!dt || !dt.length) continue;
        if (dt[0] === 1 && ix.accounts.length >= 3) initOwner.set(ix.accounts[0], kb[ix.accounts[2]]);
        else if ((dt[0] === 16 || dt[0] === 18) && dt.length >= 33) initOwner.set(ix.accounts[0], Buffer.from(dt.subarray(1, 33)));
    }
    for (const ix of all) {
        const p = kb[ix.programIdIndex], dt = Buffer.from(ix.data || []);
        if (eq(p, SYSTEM_B) && dt.length >= 12) {
            const tag = dt.readUInt32LE(0);
            if (tag === 0 && dt.length >= 52 && dt.readBigUInt64LE(4) >= min) {
                const ow = dt.subarray(20, 52); if (!(ow.equals(TOKEN_B) || ow.equals(TOKEN22_B))) continue;
                const o = initOwner.get(ix.accounts[1]); if (!o || !eq(kb[ix.accounts[0]], Buffer.from(o))) return true;
            } else if (tag === 3 && dt.length >= 44) {
                const o0 = 44 + Number(dt.readBigUInt64LE(36)); if (dt.length < o0 + 48 || dt.readBigUInt64LE(o0) < min) continue;
                const ow = dt.subarray(o0 + 16, o0 + 48); if (!(ow.equals(TOKEN_B) || ow.equals(TOKEN22_B))) continue;
                const o = initOwner.get(ix.accounts[1]); if (!o || !eq(kb[ix.accounts[0]], Buffer.from(o))) return true;
            } else if ((tag === 2 || tag === 11) && dt.readBigUInt64LE(4) >= min) {
                const to = ix.accounts[tag === 2 ? 1 : 2], t = tbo().get(to);
                if (t && t.mint === WSOL && t.owner !== bs58.encode(Buffer.from(kb[ix.accounts[0]]))) return true;
            }
        } else if (isTok(p) && dt.length >= 9 && (dt[0] === 3 || dt[0] === 12) && dt.readBigUInt64LE(1) >= min) {
            /** владелец — по балансам токенов или по InitializeAccount в этой транзакции (временный счёт, созданный и закрытый внутри неё, в балансах не виден) */
            const src = ix.accounts[0], dst = ix.accounts[dt[0] === 3 ? 1 : 2], a = tbo().get(src), b = tbo().get(dst);
            if (dt[0] === 12 && !eq(kb[ix.accounts[1]], WSOL_B)) continue;
            if (dt[0] === 3 && ((a && a.mint !== WSOL) || (b && b.mint !== WSOL))) continue;
            const ownerOf = (i, t) => t ? t.owner : initOwner.has(i) ? bs58.encode(Buffer.from(initOwner.get(i))) : null;
            const oa = ownerOf(src, a), ob = ownerOf(dst, b);
            if (oa && ob && oa !== ob && onCurve(oa) && onCurve(ob)) return true;
        } else if (isTok(p) && dt.length >= 1 && dt[0] === 9 && ix.accounts.length >= 3 && ix.accounts[1] !== ix.accounts[2]) {
            const i = ix.accounts[0], lam = (meta.preBalances || [])[i];
            if (lam !== undefined && BigInt(lam) >= min) return true;
            if (initOwner.has(i)) return true;   // создан в этой транзакции — сумму считает полный разбор
        } else if (eq(p, STAKE_B) && dt.length >= 12 && dt.readUInt32LE(0) === 4) return true;
    }
    return false;
}

/** Yellowstone gRPC (d — SubscribeUpdate с transaction) → нормализованная транзакция */
function fromGrpc(d) {
    const tx = d.transaction.transaction, msg = tx.transaction.message, meta = tx.meta || {};
    const keys = msg.accountKeys.map(k => bs58.encode(Buffer.from(k)));
    for (const k of meta.loadedWritableAddresses || []) keys.push(bs58.encode(Buffer.from(k)));
    for (const k of meta.loadedReadonlyAddresses || []) keys.push(bs58.encode(Buffer.from(k)));
    const inner = new Map(); for (const g of meta.innerInstructions || []) inner.set(g.index, g.instructions);
    const ixs = [];
    msg.instructions.forEach((ix, i) => {
        ixs.push({program: keys[ix.programIdIndex], accounts: [...ix.accounts].map(a => keys[a]), data: Buffer.from(ix.data)});
        for (const x of inner.get(i) || []) ixs.push({program: keys[x.programIdIndex], accounts: [...x.accounts].map(a => keys[a]), data: Buffer.from(x.data)});
    });
    const tb = [...(meta.preTokenBalances || []), ...(meta.postTokenBalances || [])].map(b => ({account: keys[b.accountIndex], mint: b.mint, owner: b.owner}));
    return {keys, ixs, tb, pre: (meta.preBalances || []).map(Number), post: (meta.postBalances || []).map(Number), sig: bs58.encode(Buffer.from(tx.signature))};
}

/** RPC getTransaction (encoding 'json', maxSupportedTransactionVersion 0) → нормализованная транзакция (проверки, точечная трассировка) */
function fromRpcJson(t) {
    const msg = t.transaction.message, meta = t.meta || {};
    const keys = [...msg.accountKeys, ...((meta.loadedAddresses || {}).writable || []), ...((meta.loadedAddresses || {}).readonly || [])];
    const inner = new Map(); for (const g of meta.innerInstructions || []) inner.set(g.index, g.instructions);
    const ixs = [];
    msg.instructions.forEach((ix, i) => {
        ixs.push({program: keys[ix.programIdIndex], accounts: ix.accounts.map(a => keys[a]), data: Buffer.from(bs58.decode(ix.data))});
        for (const x of inner.get(i) || []) ixs.push({program: keys[x.programIdIndex], accounts: x.accounts.map(a => keys[a]), data: Buffer.from(bs58.decode(x.data))});
    });
    const tb = [...(meta.preTokenBalances || []), ...(meta.postTokenBalances || [])].map(b => ({account: keys[b.accountIndex], mint: b.mint, owner: b.owner}));
    return {keys, ixs, tb, pre: meta.preBalances || [], post: meta.postBalances || [], sig: t.transaction.signatures[0]};
}

module.exports = {scan, quick, fromGrpc, fromRpcJson, onCurve, KINDS: {5: 'подарок WSOL', 6: 'перевод WSOL', 7: 'закрытие счёта на чужой', 8: 'вывод stake'}};
