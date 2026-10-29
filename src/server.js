/**
 * Раздача разобранных сделок по TCP (TCP_NODELAY, постоянное соединение на клиента).
 *
 * Протокол.
 *  Клиент → хаб: одна строка JSON с '\n' — {"hello": 1, "name": "micro-02-low", "subscribe": {"pump": {"bc": true, "mayhem": false, "quotes": ["SOL"]}}, "resume": {"pump_bc": 123456}}
 *    subscribe — пулы и условия (src/filters.js); старое "channels": [...] — каналы целиком (CHANNELS ниже);
 *    resume — последний полученный номер канала: хаб досылает пропущенное из кольца (HUB_RING записей, с фильтром клиента),
 *    если номера уже нет в кольце — шлёт кадр gap и продолжает с текущего.
 *  Хаб → клиент: кадры [u32 LE длина остатка][u8 тип][u8 канал][u64 LE номер][тело msgpack]
 *    тип 1 — данные (тело — запись канала), 2 — служебный (тело: {welcome}/{gap}/{ping}); номер — сквозной в канале, растёт на 1.
 *  Хаб раз в 5 с шлёт ping (номер канала 0) — клиент по тишине больше ~2 с понимает, что хаб пропал.
 *  amm_pool (27.10): сделки PumpSwap выбранных клиентом пулов (из всего потока AMM, AMM_ALL=1) — список в hello "amm_pools": [...] и дальше строками
 *    {"amm_pools": [...]} без переподключения; без кольца и дозапроса (после переподключения клиент сам перечитывает пулы); запись — {pool, slot, tx, ts, buy, q1, b1, user, sig, qm, mint, q0, b0, qa, fe}.
 *    29.10: и по котировке — "amm_quotes": [quote_mint, …] в hello / строкой {"amm_quotes": [...]}: все сделки пулов с этими котировками (qm — котировка, mint — база,
 *    q0 / b0 — резервы пула до сделки (из события), qa — quote_amount сделки, fe — lp + protocol + coin_creator комиссии в единицах котировки).
 *
 * Отставший клиент (в буфере сокета больше HUB_MAX_CLIENT_BUF_MB) отключается — остальные не ждут.
 */
const net = require('net');
const {Packr} = require('msgpackr');
const {logger} = require('./common');
const {compile} = require('./filters');

/** каналы: номер в кадре ↔ имя (номера фиксированы, клиент знает их из welcome) */
const CHANNELS = ['_ctl', 'pump_bc', 'pump_amm', 'pump_amm_pool', 'launchlab', 'launchlab_pool', 'meteora_dbc', 'meteora_dbc_pool', 'meteora_dbc_complete', 'blockhash',
    'bsc_four_meme', 'bsc_four_meme_token', 'bsc_flap', 'bsc_flap_token', 'meta', 'funding', 'farm', 'amm_pool'];   // новые — только в конец (номер канала = индекс)
/** farm — состояние ферм операторов (сборщик переводов → POST 127.0.0.1:HUB_INGEST_PORT/farm): полный снимок {epoch, ver, prob: [кошельки], ban: [[кошелёк, до]]};
 *  в кольце — несколько последних снимков, новому подписчику — сразу последний */
const RING_LIMIT = {farm: 5};
const CH_ID = Object.fromEntries(CHANNELS.map((c, i) => [c, i]));
const packr = new Packr({useRecords: false, int64AsType: 'bigint', useBigIntExtension: false});

function frame(type, ch, seq, body) {
    const payload = packr.pack(body);
    /** allocUnsafeSlow — своя память под кадр: allocUnsafe режет малые буферы из общих блоков по 8 КБ, и кадр редкого канала (мета, blockhash, BSC-токены),
     *  живущий в кольце часами, держал весь блок с давно ушедшими кадрами частых каналов — arrayBuffers хаба росли ~70 МБ/ч (10–11.10, до 685 МБ) */
    const b = Buffer.allocUnsafeSlow(4 + 1 + 1 + 8 + payload.length);
    b.writeUInt32LE(1 + 1 + 8 + payload.length, 0); b.writeUInt8(type, 4); b.writeUInt8(ch, 5); b.writeBigUInt64LE(BigInt(seq), 6); payload.copy(b, 14);
    return b;
}

function createServer({port = +(process.env.HUB_PORT || 7710), host = process.env.HUB_BIND || '0.0.0.0', ring = +(process.env.HUB_RING || 20000),
    maxBuf = +(process.env.HUB_MAX_CLIENT_BUF_MB || 64) * 1024 * 1024} = {}) {
    /** номера растут и через перезапуск хаба: старт с Date.now()*10 (больше, чем успел выдать прошлый запуск при < 10000 записей/с на канал),
     *  иначе клиент со старым последним номером отбрасывал бы новые записи как повтор */
    const seq = new Array(CHANNELS.length).fill(Date.now() * 10);
    const rings = CHANNELS.map(() => []);   // последние кадры канала: [номер, Buffer]
    const clients = new Set();
    const stats = {published: 0, bytes: 0, dropped: 0, clientsTotal: 0, ammPoolSent: 0};
    /** amm_pool: пул → сколько клиентов его ждут (быстрый отсев сделок, которые никому не нужны) */
    const poolWant = new Map(), quoteWant = new Map();   // котировка → сколько клиентов ждут все её пулы
    let wantListener = null, wantTimer = null;
    /** список ждущих пулов/котировок изменился — слушателю (поток AMM, AMM_WORKER=1) не чаще раза в 50 мс */
    const wantChanged = () => { if (!wantListener || wantTimer) return; wantTimer = setTimeout(() => { wantTimer = null; wantListener({pools: [...poolWant.keys()], quotes: [...quoteWant.keys()]}); }, 50); };
    const setWant = (map, c, key, list) => {
        for (const p of c[key] || []) { const n = (map.get(p) || 1) - 1; if (n) map.set(p, n); else map.delete(p); }
        c[key] = new Set((Array.isArray(list) ? list : []).map(String).slice(0, 2000));
        for (const p of c[key]) map.set(p, (map.get(p) || 0) + 1);
        wantChanged();
    };
    const setPools = (c, list) => setWant(poolWant, c, 'ammPools', list);
    const setQuotes = (c, list) => setWant(quoteWant, c, 'ammQuotes', list);

    const server = net.createServer(sock => {
        sock.setNoDelay(true);
        const c = {sock, name: '?', subs: new Set(), addr: `${sock.remoteAddress}:${sock.remotePort}`, sent: 0, connectedAt: Date.now()};
        let buf = '';
        sock.on('data', d => {
            buf += d.toString('utf8');
            if (c.ready) {   // после hello — только смена списка пулов amm_pool
                for (let nl = buf.indexOf('\n'); nl >= 0; nl = buf.indexOf('\n')) {
                    const line = buf.slice(0, nl); buf = buf.slice(nl + 1);
                    try { const m = JSON.parse(line); if (m.amm_pools) setPools(c, m.amm_pools); if (m.amm_quotes) setQuotes(c, m.amm_quotes); } catch (e) {}
                }
                if (buf.length > 1 << 20) sock.destroy();
                return;
            }
            const nl = buf.indexOf('\n'); if (nl < 0) { if (buf.length > 65536) sock.destroy(); return; }
            let hello; try { hello = JSON.parse(buf.slice(0, nl)); } catch (e) { sock.destroy(); return; }
            buf = buf.slice(nl + 1);
            if (hello.amm_pools) setPools(c, hello.amm_pools);
            if (hello.amm_quotes) setQuotes(c, hello.amm_quotes);
            c.name = String(hello.name || c.addr).slice(0, 64);
            c.filters = new Map();
            if (hello.subscribe) { try { for (const [ch, f] of compile(hello.subscribe)) c.filters.set(CH_ID[ch], f); } catch (e) { logger.warn(`клиент ${c.name}: фильтры ${e.message}`); } }
            for (const ch of hello.channels || []) if (CH_ID[ch] > 0) c.filters.set(CH_ID[ch], r => r);
            for (const id of c.filters.keys()) c.subs.add(id);
            sock.write(frame(2, 0, 0, {welcome: {channels: CHANNELS, subscribed: [...c.subs].map(i => CHANNELS[i]), subscribe: hello.subscribe || null}}));
            /** дозапрос из кольца */
            for (const [name, last] of Object.entries(hello.resume || {})) {
                const id = CH_ID[name]; if (!c.subs.has(id)) continue;
                const r = rings[id], from = Number(last) + 1;
                if (!r.length || from > seq[id]) continue;
                if (from < r[0][0]) sock.write(frame(2, id, seq[id], {gap: {channel: name, from, firstAvailable: r[0][0]}}));
                for (const [n, b, rec] of r) if (n >= from) { const out = c.filters.get(id)(rec); if (out) sock.write(out === rec ? b : frame(1, id, n, out)); }
            }
            /** blockhash: новому клиенту сразу последние хэши (без них он не может подписывать транзакции до следующей смены хэша) */
            const bh = CH_ID.blockhash;
            if (c.subs.has(bh) && !(hello.resume && hello.resume.blockhash)) for (const [, b] of rings[bh].slice(-30)) sock.write(b);
            const fm = CH_ID.farm;
            if (c.subs.has(fm) && rings[fm].length && !(hello.resume && hello.resume.farm)) sock.write(rings[fm][rings[fm].length - 1][1]);
            c.ready = true; clients.add(c); stats.clientsTotal++;
            logger.info(`клиент ${c.name} (${c.addr}) подписан: ${[...c.subs].map(i => CHANNELS[i]).join(', ')}${hello.subscribe ? ' ' + JSON.stringify(hello.subscribe) : ''}`);
        });
        const bye = why => { setPools(c, []); setQuotes(c, []); if (clients.delete(c)) logger.info(`клиент ${c.name} отключён (${why}), отправлено ${c.sent}`); };
        sock.on('close', () => bye('закрыт'));
        sock.on('error', e => bye(e.message));
    });
    server.listen(port, host, () => logger.info(`раздача: ${host}:${port}, каналы ${CHANNELS.slice(1).join(', ')}`));

    setInterval(() => { const p = frame(2, 0, 0, {ping: Date.now()}); for (const c of clients) c.sock.write(p); }, 5000);

    /** запись в канал: кадр один на всех подписчиков */
    function publish(channel, record) {
        const id = CH_ID[channel]; if (!id) return;
        const n = ++seq[id], b = frame(1, id, n, record);
        const r = rings[id]; r.push([n, b, record]); if (r.length > (RING_LIMIT[channel] || ring)) r.shift();
        stats.published++;
        for (const c of clients) {
            const f = c.filters.get(id); if (!f) continue;
            const out = f(record); if (!out) continue;
            if (c.sock.writableLength > maxBuf) { stats.dropped++; logger.warn(`клиент ${c.name} отстал (${(c.sock.writableLength / 1048576).toFixed(0)} МБ в буфере) — отключаю`); c.sock.destroy(); clients.delete(c); continue; }
            const fb = out === record ? b : frame(1, id, n, out);
            c.sock.write(fb); c.sent++; stats.bytes += fb.length;
        }
    }
    /** amm_pool: сделка пула — только клиентам, которые ждут этот пул или его котировку (без кольца) */
    function publishPool(record) {
        if (!poolWant.has(record.pool) && !quoteWant.has(record.qm)) return;
        const id = CH_ID.amm_pool, n = ++seq[id], b = frame(1, id, n, record);
        for (const c of clients) {
            if (!(c.ammPools && c.ammPools.has(record.pool)) && !(c.ammQuotes && c.ammQuotes.has(record.qm))) continue;
            if (c.sock.writableLength > maxBuf) continue;
            c.sock.write(b); c.sent++; stats.bytes += b.length; stats.ammPoolSent++;
        }
    }
    /** размеры колец по каналам (записей и МБ буферов) — для метрики памяти */
    const ringStats = () => Object.fromEntries(CHANNELS.map((c, i) => [c, rings[i].length]).filter(([, n]) => n > 0));
    return {publish, publishPool, wantedPools: () => [...poolWant.keys()], wantedQuotes: () => [...quoteWant.keys()], onPoolsWanted: fn => { wantListener = fn; }, stats, clients, seq, ringStats, close: () => new Promise(r => server.close(r))};
}

module.exports = {createServer, CHANNELS, CH_ID, packr};
