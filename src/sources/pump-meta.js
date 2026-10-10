/**
 * Метадата новых монет pump.fun (кривая): событие создания (Program data: G3KpTd7r… — имя, символ, uri, mint) из записи pump_bc →
 * JSON по uri через пул IPFS-шлюзов (src/ipfs-fetch.js) → канал meta → картинка (размер, sha256, pHash) → строка pump_fun_bc_meta.
 * Перенесено из copy-trade-solana sandbox/trader.js (LOG_META=1) и services/metadata.processor.js (getImage, getPerceptualHash).
 *
 * Канал meta — сразу, как пришёл JSON (картинку боты не ждут):
 *   {mint, uri, name, symbol, createdTs, fetchedAt, fetchMs, json}   — json: объект метадаты или null при ошибке (тогда error).
 *
 * Какие монеты: как писал трейдер — с хотя бы одной сделкой в SOL и не mayhem (META_ALL=1 — все созданные).
 */
const {deserialize} = require('borsh');
const bs58 = require('bs58').default;
const {createHash} = require('node:crypto');
const {logger} = require('../common');
const {fetchJson, fetchBuffer} = require('../ipfs-fetch');

const WSOL = 'So11111111111111111111111111111111111111112';
const META_ALL = process.env.META_ALL === '1';

/** начало CreateEvent: дискриминатор, name, symbol, uri, mint (остальные поля не нужны) */
const CREATE_STRUCT = {struct: {descriptor: {array: {type: 'u8', len: 8}}, name: 'string', symbol: 'string', uri: 'string', mint: {array: {type: 'u8', len: 32}}}};

function parseCreate(base64) {
    const d = deserialize(CREATE_STRUCT, Buffer.from(base64, 'base64'));
    return {name: d.name, symbol: d.symbol, uri: d.uri, mint: bs58.encode(Uint8Array.from(d.mint))};
}

/** pHash 16×16 по средней яркости → 32 байта base64 (как getPerceptualHash трейдера) */
async function perceptualHash(sharp, buffer) {
    const px = await sharp(buffer).resize(16, 16, {fit: 'fill'}).grayscale().raw().toBuffer();
    let sum = 0;
    for (const p of px) sum += p;
    const avg = sum / px.length, hash = Buffer.alloc(32);
    for (let i = 0; i < px.length; i++) if (px[i] >= avg) hash[Math.floor(i / 8)] |= 1 << (7 - (i % 8));
    return hash.toString('base64');
}

function createPumpMeta({db, write, publish}) {
    const sharp = require('sharp');
    const stats = {creates: 0, fetched: 0, failed: 0, images: 0, imageErrors: 0, written: 0, fetchMsP50: null};
    let fetchMs = [];
    const seen = new Map();   // mint → время, от повторов (кольцо/дубли)
    setInterval(() => {
        const t = Date.now() - 3600000; for (const [k, v] of seen) if (v < t) seen.delete(k);
        const s = fetchMs.sort((a, b) => a - b); stats.fetchMsP50 = s.length ? s[Math.floor(s.length / 2)] : null; fetchMs = [];
    }, 60000);

    async function onCreate(rec) {
        if (!rec.createData) return;
        if (!META_ALL && !rec.swaps.some(s => !s.mayhem_mode && (s.quote_mint === WSOL || !s.quote_mint))) return;
        let c;
        try { c = parseCreate(rec.createData); } catch (e) { logger.error(`meta: разбор создания ${rec.sig}: ${e.message}`); return; }
        if (seen.has(c.mint)) return; seen.set(c.mint, Date.now());
        stats.creates++;
        const t0 = Date.now();
        let json = null, error = null;
        try { json = await fetchJson(c.uri); } catch (e) { error = e.message; }
        const fetchedAt = Date.now();
        if (json) { stats.fetched++; fetchMs.push(fetchedAt - t0); } else stats.failed++;
        publish('meta', {mint: c.mint, uri: c.uri, name: c.name, symbol: c.symbol, createdTs: rec.ts, fetchedAt, fetchMs: fetchedAt - t0, json, ...(error ? {error} : {})});
        if (!json) { logger.debug(`meta ${c.mint}: ${error}`); return; }

        let image = {size: null, hash: null, pHash: null};
        try {
            const buf = await fetchBuffer(json.image);
            image = {size: buf.length, hash: createHash('sha256').update(buf).digest('hex'), pHash: await perceptualHash(sharp, buf)};
            stats.images++;
        } catch (e) { stats.imageErrors++; }
        if (!write) return;
        db.query('INSERT INTO pump_fun_bc_meta(mint, url, name, symbol, image, data) VALUES (?, ?, ?, ?, ?, ?)',
            [c.mint, c.uri, c.name, c.symbol, JSON.stringify(image), JSON.stringify(json)])
            .then(() => stats.written++)
            .catch(e => logger.error('pump_fun_bc_meta', e.message));
    }

    return {stats, onCreate: rec => { onCreate(rec).catch(e => logger.error('meta', e.message)); }};
}

module.exports = {createPumpMeta, parseCreate};
