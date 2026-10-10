/**
 * Раздача разобранных сделок по TCP (TCP_NODELAY, постоянное соединение на клиента).
 *
 * Протокол.
 *  Клиент → хаб: одна строка JSON с '\n' — {"hello": 1, "name": "micro-02-low", "channels": ["pump_bc"], "resume": {"pump_bc": 123456}}
 *    channels — какие каналы нужны (CHANNELS ниже); resume — последний полученный номер канала: хаб досылает пропущенное из кольца (HUB_RING записей),
 *    если номера уже нет в кольце — шлёт кадр gap и продолжает с текущего.
 *  Хаб → клиент: кадры [u32 LE длина остатка][u8 тип][u8 канал][u64 LE номер][тело msgpack]
 *    тип 1 — данные (тело — запись канала), 2 — служебный (тело: {welcome}/{gap}/{ping}); номер — сквозной в канале, растёт на 1.
 *  Хаб раз в 5 с шлёт ping (номер канала 0) — клиент по тишине больше ~2 с понимает, что хаб пропал.
 *
 * Отставший клиент (в буфере сокета больше HUB_MAX_CLIENT_BUF_MB) отключается — остальные не ждут.
 */
const net = require('net');
const {Packr} = require('msgpackr');
const {logger} = require('./common');

/** каналы: номер в кадре ↔ имя (номера фиксированы, клиент знает их из welcome) */
const CHANNELS = ['_ctl', 'pump_bc', 'pump_amm', 'pump_amm_pool', 'launchlab', 'launchlab_pool', 'meteora_dbc', 'meteora_dbc_pool', 'meteora_dbc_complete'];
const CH_ID = Object.fromEntries(CHANNELS.map((c, i) => [c, i]));
const packr = new Packr({useRecords: false, int64AsType: 'bigint', useBigIntExtension: false});

function frame(type, ch, seq, body) {
    const payload = packr.pack(body);
    const b = Buffer.allocUnsafe(4 + 1 + 1 + 8 + payload.length);
    b.writeUInt32LE(1 + 1 + 8 + payload.length, 0); b.writeUInt8(type, 4); b.writeUInt8(ch, 5); b.writeBigUInt64LE(BigInt(seq), 6); payload.copy(b, 14);
    return b;
}

function createServer({port = +(process.env.HUB_PORT || 7710), host = process.env.HUB_BIND || '0.0.0.0', ring = +(process.env.HUB_RING || 20000),
    maxBuf = +(process.env.HUB_MAX_CLIENT_BUF_MB || 64) * 1024 * 1024} = {}) {
    const seq = new Array(CHANNELS.length).fill(0);
    const rings = CHANNELS.map(() => []);   // последние кадры канала: [номер, Buffer]
    const clients = new Set();
    const stats = {published: 0, bytes: 0, dropped: 0, clientsTotal: 0};

    const server = net.createServer(sock => {
        sock.setNoDelay(true);
        const c = {sock, name: '?', subs: new Set(), addr: `${sock.remoteAddress}:${sock.remotePort}`, sent: 0, connectedAt: Date.now()};
        let buf = '';
        sock.on('data', d => {
            if (c.ready) return;
            buf += d.toString('utf8');
            const nl = buf.indexOf('\n'); if (nl < 0) { if (buf.length > 65536) sock.destroy(); return; }
            let hello; try { hello = JSON.parse(buf.slice(0, nl)); } catch (e) { sock.destroy(); return; }
            c.name = String(hello.name || c.addr).slice(0, 64);
            for (const ch of hello.channels || []) if (CH_ID[ch] > 0) c.subs.add(CH_ID[ch]);
            sock.write(frame(2, 0, 0, {welcome: {channels: CHANNELS, subscribed: [...c.subs].map(i => CHANNELS[i])}}));
            /** дозапрос из кольца */
            for (const [name, last] of Object.entries(hello.resume || {})) {
                const id = CH_ID[name]; if (!c.subs.has(id)) continue;
                const r = rings[id], from = Number(last) + 1;
                if (!r.length || from > seq[id]) continue;
                if (from < r[0][0]) sock.write(frame(2, id, seq[id], {gap: {channel: name, from, firstAvailable: r[0][0]}}));
                for (const [n, b] of r) if (n >= from) sock.write(b);
            }
            c.ready = true; clients.add(c); stats.clientsTotal++;
            logger.info(`клиент ${c.name} (${c.addr}) подписан: ${[...c.subs].map(i => CHANNELS[i]).join(', ')}`);
        });
        const bye = why => { if (clients.delete(c)) logger.info(`клиент ${c.name} отключён (${why}), отправлено ${c.sent}`); };
        sock.on('close', () => bye('закрыт'));
        sock.on('error', e => bye(e.message));
    });
    server.listen(port, host, () => logger.info(`раздача: ${host}:${port}, каналы ${CHANNELS.slice(1).join(', ')}`));

    setInterval(() => { const p = frame(2, 0, 0, {ping: Date.now()}); for (const c of clients) c.sock.write(p); }, 5000);

    /** запись в канал: кадр один на всех подписчиков */
    function publish(channel, record) {
        const id = CH_ID[channel]; if (!id) return;
        const n = ++seq[id], b = frame(1, id, n, record);
        const r = rings[id]; r.push([n, b]); if (r.length > ring) r.shift();
        stats.published++;
        for (const c of clients) {
            if (!c.subs.has(id)) continue;
            if (c.sock.writableLength > maxBuf) { stats.dropped++; logger.warn(`клиент ${c.name} отстал (${(c.sock.writableLength / 1048576).toFixed(0)} МБ в буфере) — отключаю`); c.sock.destroy(); clients.delete(c); continue; }
            c.sock.write(b); c.sent++; stats.bytes += b.length;
        }
    }
    return {publish, stats, clients, seq, close: () => new Promise(r => server.close(r))};
}

module.exports = {createServer, CHANNELS, CH_ID, packr};
