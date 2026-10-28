/**
 * Клиент хаба для ботов: подключение, подписка на каналы, переподключение с дозапросом пропущенного (resume), контроль номеров.
 *
 *   const {HubClient} = require('sol-collector-hub/client/hub-client');
 *   const hub = new HubClient({host: '10.156.0.17', port: 7710, name: 'micro-02-low',
 *       subscribe: {pump: {bc: true, amm: false, mayhem: false, quotes: ['SOL']}}});   // условия — src/filters.js
 *   hub.on('pump_bc', (rec, seq) => { ... });   // запись канала (поля — как публикует хаб: src/sources/*)
 *   hub.on('gap', g => ...);                      // пропуск, который хаб не смог дослать (кольцо переполнено)
 *   hub.on('down', ms => ...);                    // хаба нет дольше silenceMs: бот не входит в новые позиции, клиент сам переподключается
 *   hub.on('up', () => ...);
 *   hub.connect();
 *   hub.setAmmPools(['pool…', …]);   // amm_pool: сделки PumpSwap этих пулов (резервы после сделки), список меняется без переподключения
 *   hub.setAmmQuotes(['quote_mint…']); // amm_pool: все сделки пулов с этими котировками (29.10), вместе со списком пулов или вместо него
 *   hub.on('amm_pool', rec => ...);    // {pool, slot, tx, ts, buy, q1, b1, user, sig, qm, mint, qa, fe}
 */
const net = require('net');
const {EventEmitter} = require('events');
const {Unpackr} = require('msgpackr');

const unpackr = new Unpackr({useRecords: false, int64AsType: 'bigint'});

class HubClient extends EventEmitter {
    constructor({host, port = 7710, name = 'bot', subscribe = null, channels = [], silenceMs = 2000, retryMs = 500} = {}) {
        super();
        Object.assign(this, {host, port, name, subscribe, channels, silenceMs, retryMs});
        this.last = {};            // канал → последний номер
        this.names = null;         // номер канала → имя (из welcome)
        this.lastData = Date.now(); this.down = false; this.stopped = false; this.ammPools = []; this.ammQuotes = [];
        this.watch = setInterval(() => {
            const ms = Date.now() - this.lastData;
            if (ms > this.silenceMs && !this.down) { this.down = true; this.emit('down', ms); if (this.sock) this.sock.destroy(); }
        }, 250);
    }
    connect() {
        if (this.stopped) return;
        const sock = this.sock = net.connect({host: this.host, port: this.port});
        sock.setNoDelay(true);
        let buf = Buffer.alloc(0);
        sock.on('connect', () => sock.write(JSON.stringify({hello: 1, name: this.name, subscribe: this.subscribe, channels: this.channels, resume: this.last, amm_pools: this.ammPools, amm_quotes: this.ammQuotes}) + '\n'));
        sock.on('data', d => {
            this.lastData = Date.now();
            if (this.down) { this.down = false; this.emit('up'); }
            buf = buf.length ? Buffer.concat([buf, d]) : d;
            let o = 0;
            while (buf.length - o >= 4) {
                const len = buf.readUInt32LE(o); if (buf.length - o - 4 < len) break;
                const type = buf.readUInt8(o + 4), ch = buf.readUInt8(o + 5), seq = Number(buf.readBigUInt64LE(o + 6));
                const body = unpackr.unpack(buf.subarray(o + 14, o + 4 + len)); o += 4 + len;
                if (type === 2) {
                    if (body.welcome) { this.names = body.welcome.channels; this.emit('welcome', body.welcome); }
                    else if (body.gap) { this.emit('gap', body.gap); }
                    continue;
                }
                const name = this.names ? this.names[ch] : String(ch);
                const prev = this.last[name];
                if (prev !== undefined && seq <= prev) continue;                       // повтор после дозапроса
                /** разрыв номеров — не потеря: номер сквозной по каналу, а записи, не прошедшие фильтр клиента, хаб не шлёт;
                 *  внутри соединения TCP ничего не теряет, при переподключении настоящий пропуск хаб сообщает кадром gap */
                this.last[name] = seq;
                this.emit(name, body, seq);
            }
            buf = o ? buf.subarray(o) : buf;
        });
        const retry = () => { if (!this.stopped) setTimeout(() => this.connect(), this.retryMs); };
        sock.on('close', retry);
        sock.on('error', () => {});
    }
    /** пулы канала amm_pool: сразу хабу (если соединение есть) и в hello при переподключении */
    setAmmPools(list) {
        this.ammPools = [...list];
        if (this.sock && !this.sock.destroyed && this.sock.writable && !this.sock.connecting) this.sock.write(JSON.stringify({amm_pools: this.ammPools}) + '\n');
    }
    /** котировки канала amm_pool (29.10): все пулы с этими котировками — так же, как setAmmPools */
    setAmmQuotes(list) {
        this.ammQuotes = [...list];
        if (this.sock && !this.sock.destroyed && this.sock.writable && !this.sock.connecting) this.sock.write(JSON.stringify({amm_quotes: this.ammQuotes}) + '\n');
    }
    stop() { this.stopped = true; clearInterval(this.watch); if (this.sock) this.sock.destroy(); }
}

module.exports = {HubClient};
