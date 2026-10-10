/** Перенесено из copy-trade-solana services/ipfs-fetch.js (без изменений логики). */
/**
 * Загрузка метаданных/картинок с IPFS через пул публичных шлюзов.
 *
 * Публичный шлюз ipfs.io отдаёт 429, если стучаться в него со всей скоростью
 * потока новых минтов pump.fun. Поэтому:
 *  - один и тот же CID можно взять с любого шлюза, пробуем их по кругу;
 *  - шлюз, ответивший 429 (или 5xx), уходит в cooldown и до истечения
 *    cooldown'а вообще не участвует в выборе — это и убирает шторм 429;
 *  - запросы к одному хосту сериализуются с минимальным интервалом, чтобы
 *    не провоцировать лимит;
 *  - ответы кешируются по CID, один минт не тянет один и тот же JSON дважды.
 *
 * Список шлюзов переопределяется IPFS_GATEWAYS (через запятую, каждый должен
 * заканчиваться на /ipfs/). Если есть выделенный шлюз Pinata, он ставится
 * первым: PINATA_GATEWAY=my-name.mypinata.cloud [+ PINATA_GATEWAY_TOKEN].
 */

const DEFAULT_GATEWAYS = [
    /** Порядок — по замеру 25.09.2026: ipfs.io/dweb.link/nftstorage.link уже лимитят нас 429 */
    'https://ipfs.filebase.io/ipfs/',
    'https://4everland.io/ipfs/',
    'https://gateway.pinata.cloud/ipfs/',
    'https://ipfs.io/ipfs/',
    'https://dweb.link/ipfs/',
    'https://nftstorage.link/ipfs/',
];

const MIN_HOST_GAP_MS = parseInt(process.env.IPFS_MIN_HOST_GAP_MS || '150', 10);
const REQUEST_TIMEOUT_MS = parseInt(process.env.IPFS_TIMEOUT_MS || '10000', 10);
const COOLDOWN_MS = parseInt(process.env.IPFS_COOLDOWN_MS || '30000', 10);
const MAX_COOLDOWN_MS = parseInt(process.env.IPFS_MAX_COOLDOWN_MS || '300000', 10);
const CACHE_TTL_MS = parseInt(process.env.IPFS_CACHE_TTL_MS || '600000', 10);

const gateways = (() => {
    const list = [];
    if (process.env.PINATA_GATEWAY) {
        const host = process.env.PINATA_GATEWAY.replace(/^https?:\/\//, '').replace(/\/.*$/, '');
        list.push(`https://${host}/ipfs/`);
    }
    const configured = (process.env.IPFS_GATEWAYS || '')
        .split(',')
        .map(s => s.trim())
        .filter(Boolean)
        .map(s => (s.endsWith('/') ? s : s + '/'));
    list.push(...(configured.length ? configured : DEFAULT_GATEWAYS));

    return [...new Set(list)];
})();

/** Состояние хостов: когда можно снова стучаться и сколько раз подряд лимитило */
const hostState = new Map();

const getHostState = (host) => {
    let state = hostState.get(host);
    if (!state) {
        state = {cooldownUntil: 0, failures: 0, queue: Promise.resolve(), lastAt: 0};
        hostState.set(host, state);
    }
    return state;
};

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

/**
 * Извлекает CID и остаток пути из любого вида ipfs-ссылки.
 *
 * @param {string} uri
 * @returns {{cid: string, path: string}|null}
 */
const parseIpfsUri = (uri) => {
    if (typeof uri !== 'string') {
        return null;
    }
    let match = uri.match(/^ipfs:\/\/(?:ipfs\/)?([^/?#]+)(\/[^?#]*)?/i);
    if (match) {
        return {cid: match[1], path: match[2] || ''};
    }
    match = uri.match(/^https?:\/\/[^/]+\/ipfs\/([^/?#]+)(\/[^?#]*)?/i);
    if (match) {
        return {cid: match[1], path: match[2] || ''};
    }
    /** Subdomain-шлюзы: https://<cid>.ipfs.<host>/<path> */
    match = uri.match(/^https?:\/\/([a-z0-9]+)\.ipfs\.[^/]+(\/[^?#]*)?/i);
    if (match) {
        return {cid: match[1], path: match[2] || ''};
    }
    return null;
};

/**
 * Список URL-кандидатов для запроса: для ipfs — все живые шлюзы по кругу,
 * для обычной ссылки — она сама.
 *
 * @param {string} uri
 * @returns {string[]}
 */
let rotation = 0;
const candidateUrls = (uri) => {
    const parsed = parseIpfsUri(uri);
    if (!parsed) {
        return [uri];
    }

    const suffix = parsed.cid + parsed.path;
    const now = Date.now();
    const offset = rotation++ % gateways.length;
    const ordered = [];
    for (let i = 0; i < gateways.length; i++) {
        ordered.push(gateways[(offset + i) % gateways.length]);
    }

    const ready = ordered.filter(g => getHostState(new URL(g).host).cooldownUntil <= now);
    /** Если остыть не успел никто — всё равно пробуем, но начиная с самого «свежего» */
    const fallback = ordered
        .slice()
        .sort((a, b) => getHostState(new URL(a).host).cooldownUntil - getHostState(new URL(b).host).cooldownUntil);

    return (ready.length ? ready : fallback).map(g => g + suffix);
};

/**
 * Один запрос к хосту с соблюдением минимального интервала между запросами.
 *
 * @param {string} url
 * @param {object} headers
 * @returns {Promise<Response>}
 */
const throttledFetch = (url, headers) => {
    const host = new URL(url).host;
    const state = getHostState(host);

    const run = async () => {
        const wait = state.lastAt + MIN_HOST_GAP_MS - Date.now();
        if (wait > 0) {
            await sleep(wait);
        }
        state.lastAt = Date.now();

        const extra = {};
        if (process.env.PINATA_GATEWAY_TOKEN && host === (process.env.PINATA_GATEWAY || '').replace(/^https?:\/\//, '').replace(/\/.*$/, '')) {
            extra['x-pinata-gateway-token'] = process.env.PINATA_GATEWAY_TOKEN;
        }

        return fetch(url, {
            headers: {...headers, ...extra},
            signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
        });
    };

    const result = state.queue.then(run, run);
    /** Очередь не должна ломаться из-за упавшего запроса */
    state.queue = result.then(() => {}, () => {});

    return result;
};

/**
 * Помечает хост как перегруженный: cooldown растёт экспоненциально,
 * Retry-After уважается.
 *
 * @param {string} host
 * @param {string|null} retryAfter
 */
const cooldown = (host, retryAfter) => {
    const state = getHostState(host);
    state.failures++;
    let ms = Math.min(MAX_COOLDOWN_MS, COOLDOWN_MS * Math.pow(2, state.failures - 1));
    const seconds = retryAfter ? parseInt(retryAfter, 10) : NaN;
    if (Number.isFinite(seconds) && seconds > 0) {
        ms = Math.max(ms, Math.min(MAX_COOLDOWN_MS, seconds * 1000));
    }
    state.cooldownUntil = Date.now() + ms;
};

/**
 * Достаёт ресурс, перебирая шлюзы и переживая 429/5xx.
 *
 * @param {string} uri
 * @param {object} [options]
 * @param {string} [options.accept]
 * @param {number} [options.rounds] сколько раз пройти по всему списку шлюзов
 * @returns {Promise<Response>} успешный ответ (body ещё не прочитан)
 */
const fetchFromGateways = async (uri, {accept = '*/*', rounds = 3} = {}) => {
    const errors = [];

    for (let round = 0; round < rounds; round++) {
        const urls = candidateUrls(uri);
        for (const url of urls) {
            const host = new URL(url).host;
            try {
                const response = await throttledFetch(url, {accept});

                if (response.status === 429 || response.status >= 500) {
                    cooldown(host, response.headers.get('retry-after'));
                    errors.push(`${host} HTTP ${response.status}`);
                    continue;
                }
                if (!response.ok) {
                    /** 404/410 — контента нет ни на одном шлюзе, хост не виноват */
                    errors.push(`${host} HTTP ${response.status}`);
                    continue;
                }

                getHostState(host).failures = 0;
                return response;
            } catch (e) {
                cooldown(host, null);
                errors.push(`${host} ${e.message}`);
            }
        }

        if (round + 1 < rounds) {
            await sleep(Math.min(MAX_COOLDOWN_MS, 1000 * Math.pow(2, round)));
        }
    }

    throw new Error(`unable to fetch ${uri}: ${errors.join('; ')}`);
};

/** Кеш JSON по CID (или по url, если это не ipfs) */
const jsonCache = new Map();

/**
 * @param {string} uri
 * @returns {Promise<object>}
 */
const fetchJson = async (uri) => {
    const parsed = parseIpfsUri(uri);
    const key = parsed ? parsed.cid + parsed.path : uri;
    const cached = jsonCache.get(key);
    if (cached && cached.expiresAt > Date.now()) {
        return cached.promise;
    }

    const promise = (async () => {
        const startedAt = Date.now();
        const response = await fetchFromGateways(uri, {accept: 'application/json'});
        const contentType = response.headers.get('content-type') || '';
        const text = await response.text();

        let result;
        try {
            result = JSON.parse(text);
        } catch (e) {
            throw new Error(`not a json (${contentType}) ${uri}`);
        }
        if (!result || typeof result !== 'object') {
            throw new Error(`not a json object ${uri}`);
        }

        result.startedAt = startedAt;
        result.loadedAt = Date.now();
        result.loadedIn = result.loadedAt - result.startedAt;

        return result;
    })();

    jsonCache.set(key, {promise, expiresAt: Date.now() + CACHE_TTL_MS});
    promise.catch(() => jsonCache.delete(key));

    if (jsonCache.size > 5000) {
        for (const [k, v] of jsonCache) {
            if (v.expiresAt <= Date.now()) {
                jsonCache.delete(k);
            }
        }
    }

    return promise;
};

/**
 * @param {string} uri
 * @returns {Promise<Buffer>}
 */
const fetchBuffer = async (uri) => {
    const response = await fetchFromGateways(uri);
    return Buffer.from(await response.arrayBuffer());
};

module.exports = {
    fetchJson,
    fetchBuffer,
    fetchFromGateways,
    parseIpfsUri,
    gateways,
};
