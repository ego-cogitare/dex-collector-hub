/** Проверочный клиент: node scripts/listen.js host port канал[,канал] секунд — счётчики, пример записи, задержка ts→приход */
const {HubClient} = require('../client/hub-client');
const [host = '127.0.0.1', port = 7710, chans = 'pump_bc', sec = 30] = process.argv.slice(2);
const hub = new HubClient({host, port: +port, name: 'listen-test', channels: chans.split(',')});
const n = {}, lag = []; let sample = {};
for (const c of chans.split(',')) hub.on(c, rec => { n[c] = (n[c] || 0) + 1; if (rec.ts) lag.push(Date.now() - rec.ts); if (!sample[c]) sample[c] = rec; });
hub.on('gap', g => console.log('gap', g)); hub.on('welcome', w => console.log('welcome', w.subscribed));
hub.connect();
setTimeout(() => {
  const s = lag.sort((a, b) => a - b), q = p => s[Math.floor(p * s.length)];
  console.log('записей', n, '| ts→приход мс p50', q(.5), 'p99', q(.99));
  console.log('пример', JSON.stringify(sample, (k, v) => typeof v === 'bigint' ? v.toString() + 'n' : v).slice(0, 1500));
  process.exit(0);
}, sec * 1000);
