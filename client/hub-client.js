/**
 * Клиент хаба для ботов: подключение, подписка на каналы, переподключение с дозапросом пропущенного (resume), контроль номеров.
 *
 *   const {HubClient} = require('sol-collector-hub/client/hub-client');
 *   const hub = new HubClient({host: '10.156.0.17', port: 7710, name: 'micro-02-low', channels: ['pump_bc']});
 *   hub.on('pump_bc', (rec, seq) => { ... });   // запись канала (поля — как публикует хаб: src/sources/*)
 *   hub.on('gap', g => ...);                      // пропуск, который хаб не смог дослать (кольцо переполнено)
 *   hub.on('down', ms => ...);                    // хаба нет дольше silenceMs — бот может переключиться на прямую подписку
 *   hub.on('up', () => ...);
 *   hub.connect();
 */
const net = require('net');
const {EventEmitter} = require('events');
const {Unpackr} = require('msgpackr');

const unpackr = new Unpackr({useRecords: false, int64AsType: 'bigint'});

class HubClient extends EventEmitter {
    constructor({host, port = 7710, name = 'bot', channels = [], silenceMs = 2000, retryMs = 500} = {}) {
        super();
        Object.assign(this, {host, port, name, channels, silenceMs, retryMs});
        this.last = {};            // канал → последний номер
        this.names = null;         // номер канала → имя (из welcome)
        this.lastData = Date.now(); this.down = false; this.stopped = false;
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
        sock.on('connect', () => sock.write(JSON.stringify({hello: 1, name: this.name, channels: this.channels, resume: this.last}) + '\n'));
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
                if (prev !== undefined && seq !== prev + 1) this.emit('gap', {channel: name, from: prev + 1, to: seq - 1});
                this.last[name] = seq;
                this.emit(name, body, seq);
            }
            buf = o ? buf.subarray(o) : buf;
        });
        const retry = () => { if (!this.stopped) setTimeout(() => this.connect(), this.retryMs); };
        sock.on('close', retry);
        sock.on('error', () => {});
    }
    stop() { this.stopped = true; clearInterval(this.watch); if (this.sock) this.sock.destroy(); }
}

module.exports = {HubClient};
