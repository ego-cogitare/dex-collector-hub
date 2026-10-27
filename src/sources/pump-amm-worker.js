/**
 * Источник pump.fun AMM в отдельном потоке выполнения (worker_threads, своё ядро) — AMM_WORKER=1 (27.10).
 * С AMM_ALL=1 поток всей программы AMM (~13 тыс. транзакций/мин) разбирался в том же цикле событий, что и раздача ботам: хвост задержки
 * основного потока (createdAt → приход) вырос с p99 ~30–70 мс до 450–820 мс вечером. Теперь в потоке AMM — gRPC Chainstack, разбор, запись в MySQL
 * (своё соединение), «скрытые» движения; в основной поток уходят только записи для раздачи:
 *   {t: 'pub', ch, r}  — каналы pump_amm / pump_amm_pool (пулы окна AMM_WINDOW_MIN)
 *   {t: 'pool', r}     — amm_pool: сделки пулов, которые ждут клиенты (список — из основного потока: {t: 'want', pools})
 * Остановка: {t: 'stop'} → поток дописывает буферы и отвечает {t: 'stopped'}. Поток упал — основной поток запускает его заново через 5 с.
 */
'use strict';
const {Worker, isMainThread, parentPort, workerData} = require('worker_threads');

if (isMainThread) {
    const {logger} = require('../common');
    /** запуск из src/index.js: publish — раздача (с обвязкой index.js), hub — сервер (publishPool, список ждущих пулов) */
    function startPumpAmmWorker({write = true, publish, hub}) {
        let worker = null, stopping = false, onStopped = null;
        const spawn = () => {
            worker = new Worker(__filename, {workerData: {write}});
            worker.on('message', m => {
                if (m.t === 'pub') publish(m.ch, m.r);
                else if (m.t === 'pool') hub.publishPool(m.r);
                else if (m.t === 'stopped' && onStopped) onStopped();
            });
            worker.on('error', e => logger.error(`поток AMM: ${e.stack || e.message}`));
            worker.on('exit', code => {
                if (stopping) { if (onStopped) onStopped(); return; }
                logger.error(`поток AMM завершился (код ${code}) — перезапуск через 5 с`);
                setTimeout(spawn, 5000);
            });
            worker.postMessage({t: 'want', pools: hub.wantedPools()});
            logger.info('поток AMM запущен (worker_threads)');
        };
        hub.onPoolsWanted(pools => { if (worker) worker.postMessage({t: 'want', pools}); });
        spawn();
        return {
            stop: () => new Promise(resolve => {
                stopping = true; onStopped = resolve;
                try { worker.postMessage({t: 'stop'}); } catch (e) { resolve(); }
                setTimeout(() => { try { worker.terminate(); } catch (e) {} resolve(); }, 4500);
            }),
        };
    }
    module.exports = {startPumpAmmWorker};
} else {
    require('dotenv').config({path: require('path').join(__dirname, '../../.env'), quiet: true});
    const mysql = require('mysql2/promise');
    const {logger} = require('../common');
    logger.level = process.env.LOG_LEVEL || 'info';
    (async () => {
        const db = mysql.createPool({host: process.env.MYSQL_HOST, user: process.env.MYSQL_USERNAME, password: process.env.MYSQL_PASSWORD,
            database: process.env.MYSQL_DATABASE, connectionLimit: 3, charset: 'utf8mb4'});
        let want = new Set();
        const amm = await require('./pump-amm-runner').startPumpAmm({db, write: workerData.write,
            publish: (ch, r) => parentPort.postMessage({t: 'pub', ch, r}),
            publishPool: r => { if (want.has(r.pool)) parentPort.postMessage({t: 'pool', r}); }});
        parentPort.on('message', async m => {
            if (m.t === 'want') want = new Set(m.pools);
            else if (m.t === 'stop') {
                await Promise.race([amm.stop(), new Promise(r => setTimeout(r, 4000))]);
                await db.end().catch(() => {});
                parentPort.postMessage({t: 'stopped'});
                process.exit(0);
            }
        });
    })().catch(e => { logger.error(`поток AMM: ${e.stack || e.message}`); process.exit(1); });
}
