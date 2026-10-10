/** Перенесено из copy-trade-solana services/funding-tracer.js (без изменений логики). */
/**
 * funding_tracer.js — класс-обёртка фандинг-трейса для подключения в trader.js.
 *
 * Делает то же, что funding_trace.js (v3, с lastFund/recentIns), но как модуль:
 *   - один инстанс на процесс, кэш хопов персистентный (append jsonl);
 *   - основной метод: trace(wallet, t0, mint) -> результат цепочки (+ пишется в outFile);
 *   - traceHolders(mint, t0, wallets[]) -> удобный батч для криейтора + первых 6 держателей;
 *   - inflight-дедупликация: общий родитель у двух держателей трейсится ОДИН раз.
 *
 * ПОДКЛЮЧЕНИЕ:
 *   const FundingTracer = require('./funding_tracer');
 *   const tracer = new FundingTracer({ apiKey: process.env.HELIUS_API_KEY });
 *
 *   // в момент появления минта (t0 = timestamp первого свопа, мс):
 *   const holders6 = topHolders.slice(0, 6).map(h => h.wallet);
 *   tracer.traceHolders(mint, t0, [creator, ...holders6])   // fire-and-forget
 *         .then(rs => { /* rs: массив результатов, уже записаны в outFile *\/ });
 *
 *   // или точечно:
 *   const r = await tracer.trace(creatorWallet, t0, mint);
 *   // r.lastFundAgeSec  — секунд от последней зарядки до t0 (сайзинг-горб: 10м-24ч зона)
 *   // r.creatorAgeSec   — возраст первого фандинга (null если fat/aged)
 *   // r.terminalType    — cex | aged/creator_aged | fat/creator_fat | cluster | dead_end | depth_limit
 *
 * ВАЖНО для риалтайма: полный трейс = секунды RPC. НЕ блокируй им вход в сделку —
 * собирай асинхронно; для мгновенного сайзинга по lastFund хватает { maxDepth: 1 }.
 */

'use strict';

const fs = require('fs');

const DEFAULTS = {
    maxDepth: 3,             // максимум хопов вверх. Каждый хоп — это ~8 RPC-вызовов на
                             // кошелёк (страницы сигнатур + txLookup транзакций), а лимит
                             // ключей упирается именно в это; глубины 3 хватает для
                             // решения, дальше цепочка почти всегда упирается в cex/fat.
    agedDays: 7,             // кошелёк старше N дней до t0 => терминатор aged (каузально)
    agedSigs: 300,           // >= N подписей => fat (историю не листаем; НЕ сигнал возраста)
    sigPageLimit: 1000,
    maxSigPages: 3,
    txLookup: 5,             // самых ранних tx — поиск первого пополнения
    lastLookup: 25,          // новейших tx — поиск недавних пополнений (шире: активные боты
                             // успевают наторговать поверх зарядки)
    lastKeep: 8,             // сколько новейших входящих храним
    retries: 5,
    concurrency: 6,          // параллелизм внутри traceHolders
    cacheFile: 'funding_cache.jsonl',
    outFile: 'funding_results.jsonl',
    onResult: null,          // функция(rec): куда отдавать результаты вместо outFile (бот пишет их в MySQL pump_fun_funding)
    rpcUrl: null,            // либо задаётся apiKey / apiKeys
    apiKey: null,
    apiKeys: null,           // массив ключей -> round-robin на каждый RPC-вызов
    maxConcurrentTraces: 32, // ГЛОБАЛЬНЫЙ лимит одновременных трейсов (слоты теперь короткие)
    maxConcurrentRpc: 60,    // одновременных RPC в полёте (верхняя крышка)
    perKeyRps: 6,            // ПОТОЛОК вызовов/сек НА КЛЮЧ. Замер 25.09.2026 на реальной
                             // смеси трейса (getSignaturesForAddress limit 1000 +
                             // getTransaction) на free Helius: 6/с на ключ = 0 из 592
                             // запросов с 429, 8/с = 2%, 9/с и выше — заметно хуже.
                             // Чистый getTransaction держит и 10/с, но тяжёлые методы
                             // стоят дороже, поэтому потолок ставим по худшей смеси.
                             // Платный Developer = 50 -> можно ставить 45.
                             // null = выключено (слепой round-robin без ограничителя).
    minPerKeyRps: 2,         // нижняя граница адаптации: до неё опускается темп ключа,
                             // который отвечает 429
    lowerFactor: 0.8,        // во сколько режем темп ключа на 429 (мягко: одиночный 429
                             // на границе окна не должен ронять ключ в пол)
    raiseStep: 0.5,          // на сколько rps поднимаем ключ после серии успехов
    raiseAfter: 10,          // сколько успешных запросов подряд для подъёма темпа
    riChunk: 6,              // recentIns тянем порциями по N транзакций и прекращаем, как
                             // только набрали lastKeep входящих (экономит ~треть вызовов
                             // на кошелёк). null = как раньше, всё окно разом.
    batchSize: 1,            // вызовов в одном JSON-RPC batch. 1 = батчинг ВЫКЛЮЧЕН.
                             // Замер 25.09.2026 на free Helius: батч из 9 getTransaction,
                             // единственный в секундном окне ключа, всё равно получает 429 —
                             // батч тарифицируется строже, чем те же 9 одиночных вызовов
                             // (одиночными: 8 из 600 запросов, батчами: 119 из 198).
                             // Поднимать только если проверишь на своём тарифе.
    rpcTimeoutMs: 20000,     // таймаут HTTP-запроса к RPC
    burstFactor: 1,          // НЕ ИСПОЛЬЗУЕТСЯ (оставлен для совместимости вызовов):
                             // ограничитель перешёл со токен-бакета на скользящее окно,
                             // а любой burst сверх perKeyRps как раз и давал 429.
    buyerLastLookup: 10,     // окно recentIns для ПОКУПАТЕЛЕЙ (короче: -40% запросов на минт);
                             // криейтору остаётся полный lastLookup
    serviceChildLimit: 30,   // кормилец >= N разных кошельков = сервис-раздатчик (Axiom и пр.):
                             // не считается оператором в сетях минтов
    dedupePerMint: true,     // не трейсить один кошелёк дважды в рамках одного минта
    recentInsMode: 'all',    // recentIns ВСЕМ ролям: связки через свежие зарядки старых
                             // кошельков-покупателей — ключевой операторский паттерн
    recentTtlMs: 0,          // повторная встреча = всегда освежить снимок зарядок
    noneTtlMs: 0,            // пустой-на-снимке кошелёк = всегда перепроверить
    // recentIns (lastFund) — самая дорогая часть трейса (до lastLookup getTransaction на кошелёк).
    // Сайзинг-сигнал lastFund значим только для КРИЕЙТОРА; держателям хватает firstTs/fat-класса.
    // 'creator-only': в traceHolders recentIns собирается только для ПЕРВОГО кошелька массива
    // (клади криейтора первым!), в trace() — управляется per.wantRecent (default true).
};

const KNOWN_CEX = new Set([
    '2ojv9BAiHUrvsm9gxDe7fJSzbNZSJcxZvf8dqmWGHG8S', // Binance 1
    '5tzFkiKscXHK5ZXCGbXZxdw7gTjjD1mBwuoFbhUvuAi9', // Binance 2
    '9WzDXwBbmkg8ZTbNMqUxvQRAyrZzDsGYdLVL9zYtAWWM', // Binance 3
    '3gd3dqgtJ4jWfBfLYTX67DALFetjc5iS72sCgRhCkW2u', // Binance 10
    '6QJzieMYfp7yr3EdrePaQoG3Ghxs2wM98xSLRu8Xh56U', // Binance 11
    'iGdFcQoyR2MwbXMHQskhmNsqddZ6rinsipHc4TNSdwu',  // Bybit Wallet 10
    'ASTyfSima4LLAdDgoFGkgqoKowG1LZFDr9fAQrg7iaJZ', // MEXC
    '9obNtb5GyUegcs3a1CbBkLuc5hEWynWfJC6gjz5uWQkE', // Coinbase 4
    'H8sMJSCQxfKiFTCfDR3DUMLPwcRbM61LGFJ8N4dK3WjS', // Coinbase 1
    '2AQdpHJ2JpcEgPiATUXjQxA8QmafFegfQwSLWSprPicm', // Coinbase 2
    '5g7yNHyGLJ7fiQ9SN9mf47opDnMjc585kqXWt6d7aBWs', // Coinbase Hot
    'AC5RDfQFmDS1deWZos921JfqscXdByf8BKHs5ACWjtW2', // Bybit Hot
    'A77HErqtfN1hLLpvZ9pCtu66FEtM8BveoaKbbMoZ4RiR', // Bitget
]);

const sleep = ms => new Promise(r => setTimeout(r, ms));

class FundingTracer {

    constructor(opts = {}) {
        this.cfg = { ...DEFAULTS, ...opts };
        const keys = this.cfg.apiKeys && this.cfg.apiKeys.length ? this.cfg.apiKeys
            : (this.cfg.apiKey ? [this.cfg.apiKey] : null);
        if (this.cfg.rpcUrl) this.rpcUrls = [this.cfg.rpcUrl];
        else if (keys) this.rpcUrls = keys.map(k => `https://mainnet.helius-rpc.com/?api-key=${k}`);
        else throw new Error('FundingTracer: нужен apiKey, apiKeys[] либо rpcUrl');
        this._rrIdx = 0;                                   // round-robin указатель
        this.rpcCallsPerKey = new Array(this.rpcUrls.length).fill(0);
        // токен-бакеты per key: {tokens, last}
        /** Ограничитель на ключ — СКОЛЬЗЯЩЕЕ ОКНО 1 с, ровно как считает Helius.
         *  Токен-бакет здесь не годился: за простой между минтами он накапливал
         *  запас и на первом же минте выпускал его залпом, из-за чего в одну
         *  секунду уходило больше вызовов, чем разрешено (замер 25.09.2026:
         *  10 вызовов/с на ключ проходят чисто, 15/с дают ~50% 429; лимит
         *  считается по ВЫЗОВАМ, батч из 25 ловит 429 сразу). */
        this._buckets = this.rpcUrls.map(() => ({
            hits: [],                       // {t, w} вызовы за последнюю секунду
            used: 0,                        // сумма веса в окне
            lastAt: 0,                      // когда ушёл последний вызов (равномерный шаг)
            rps: this.cfg.perKeyRps || 0,   // текущий (адаптивный) потолок ключа
            successes: 0,
            cooldownUntil: 0,
        }));
        /** Батч не должен превышать секундный бюджет ключа, иначе он не влезет в окно */
        this._batchSize = this.cfg.perKeyRps
            ? Math.max(1, Math.min(this.cfg.batchSize, Math.floor(this.cfg.perKeyRps)))
            : this.cfg.batchSize;
        this._tracesActive = 0;                            // глобальный семафор трейсов
        this._traceQueue = [];
        this._rpcActive = 0;                               // глобальный семафор RPC
        this._rpcQueue = [];
        this._tracedPerMint = new Map();                   // mint -> Set(wallet), дедуп вызовов
        this._mintNodes = new Map();                       // mint -> Set(узлы фандинг-сети минта)
        this._feederKids = new Map();                      // кормилец -> Set(кошельки) для сервис-детекта
        this.rpcCalls = 0;                                 // логических JSON-RPC вызовов
        this.rpcRequests = 0;                              // HTTP-запросов (батч = 1)
        this.rpc429 = 0;                                   // сколько раз словили rate limit
        this.hopCache = new Map();      // address -> hop result (инвариантные факты)
        this.inflight = new Map();      // address -> Promise (дедупликация параллельных запросов)
        this.nodeIndex = new Set();     // узлы прежних цепочек (cluster-детекция, в памяти процесса)
        this._loadCache();
        this.cacheOut = fs.createWriteStream(this.cfg.cacheFile, { flags: 'a' });
        this.resultOut = this.cfg.onResult ? null : fs.createWriteStream(this.cfg.outFile, { flags: 'a' });
    }

    /** результат трейса — в onResult, если задан, иначе строкой в outFile */
    _emit(rec) {
        if (this.cfg.onResult) {
            try { this.cfg.onResult(rec); } catch (e) {}
            return;
        }
        this.resultOut.write(JSON.stringify(rec) + '\n');
    }

    /** =============== ПУБЛИЧНОЕ API =============== */

    /**
     * Полный трейс одного кошелька.
     * @param {string} wallet — адрес (криейтор или держатель)
     * @param {number} t0 — момент решения, мс (timestamp первого свопа минта); default: сейчас
     * @param {string|null} mint — для записи в результат (опционально)
     * @param {object} per — оверрайды на вызов, напр. { maxDepth: 1 } для быстрого lastFund
     * @returns {Promise<object>} результат (также дописан строкой в outFile)
     */
    async trace(wallet, t0 = Date.now(), mint = null, per = {}) {
        if (this.cfg.dedupePerMint && mint) {
            let set = this._tracedPerMint.get(mint);
            if (!set) { set = new Set(); this._tracedPerMint.set(mint, set); }
            if (set.has(wallet)) return { mint, wallet, t0, skipped: 'already_traced' };
            set.add(wallet);
        }
        const requestedAt = Date.now();   // момент, когда бот РЕШИЛ тянуть (до очереди!)
        await this._acquireSlot(per.priority === true);
        try {
            const wantRecent = per.wantRecent !== undefined ? per.wantRecent
                : this.cfg.recentInsMode !== 'none';
            const r = await this._traceFunding(wallet, t0, { ...this.cfg, ...per, wantRecent });
            if (mint) this._indexMintNodes(mint, wallet, r);
            const tracedAt = Date.now();
            /** КАУЗАЛЬНОСТЬ ДЛЯ СИМОВ: данные этой записи существуют для бота с момента
             *  tracedAt. В симе их можно использовать только в решениях со временем >= tracedAt.
             *  traceMs (в т.ч. очередь) — латентность для моделирования доступности
             *  при доборах по историческим дням. */
            const rec = { mint, wallet, t0, requestedAt, tracedAt, traceMs: tracedAt - requestedAt, ...r };
            this._emit(rec);
            return rec;
        } finally {
            this._releaseSlot();
        }
    }

    _acquireSlot(priority = false) {
        if (this._tracesActive < this.cfg.maxConcurrentTraces) {
            this._tracesActive++;
            return Promise.resolve();
        }
        return new Promise(res => {
            if (priority) this._traceQueue.unshift(res);   // криейторы — в голову очереди
            else this._traceQueue.push(res);
        }).then(() => { this._tracesActive++; });
    }

    _releaseSlot() {
        this._tracesActive--;
        const next = this._traceQueue.shift();
        if (next) next();
    }

    /** Забыть минт (вызывай при закрытии позиции/смерти минта — чистит дедуп-память). */
    forgetMint(mint) { this._tracedPerMint.delete(mint); this._mintNodes.delete(mint); }

    /** Пополняет фандинг-сеть минта узлами цепочки (CEX не индексируем — общий источник всех). */
    _noteFeeder(feeder, kid) {
        let kids = this._feederKids.get(feeder);
        if (!kids) { kids = new Set(); this._feederKids.set(feeder, kids); }
        kids.add(kid);
        return kids.size;
    }

    _isService(addr) {
        const kids = this._feederKids.get(addr);
        return !!kids && kids.size >= this.cfg.serviceChildLimit;
    }

    _indexMintNodes(mint, wallet, r) {
        let set = this._mintNodes.get(mint);
        if (!set) { set = new Set(); this._mintNodes.set(mint, set); }
        set.add(wallet);
        for (const h of (r.chain || [])) {
            set.add(h.wallet);
            if (!KNOWN_CEX.has(h.parent)) {
                this._noteFeeder(h.parent, h.wallet);
                if (!this._isService(h.parent)) set.add(h.parent);
            }
        }
        // связи через недавние зарядки (recentIns с отправителями) — узлы-кормильцы
        const hop = this.hopCache.get(wallet);
        if (hop && Array.isArray(hop.recentIns)) {
            for (const x of hop.recentIns) {
                if (x && typeof x === 'object' && x.f && !KNOWN_CEX.has(x.f)) {
                    this._noteFeeder(x.f, wallet);
                    if (!this._isService(x.f)) set.add(x.f);
                }
            }
        }
    }

    /**
     * ПОЗДНИЙ ПОКУПАТЕЛЬ из уже вытянутой сети минта?
     * Уровень 1 (0 RPC): сам адрес фигурирует в фандинг-сети первых 6/криейтора.
     * Уровень 2 (~3-6 RPC, opts.deep=true): его РОДИТЕЛЬ (1 хоп) состоит в сети.
     * Возвращает { affiliated, direct, viaParent, netSize }.
     */
    async checkLateBuyer(mint, t0, wallet, opts = {}) {
        const net = this._mintNodes.get(mint);
        if (!net || net.size === 0) return { affiliated: false, direct: false, viaParent: false, netSize: 0 };
        /** сервисность проверяется в момент ЧТЕНИЯ (ретроактивно): раздатчик мог
         *  попасть в сеть раньше, чем набрал порог детей */
        const inNet = (a) => net.has(a) && !this._isService(a);
        const direct = inNet(wallet);
        let viaParent = false, viaRecent = false;
        if (!direct && opts.deep !== false) {
            const hop = await this._firstIncoming(wallet, true);    // 1 хоп, СО свежими зарядками
            if (hop) {
                if (hop.parent && inNet(hop.parent)) viaParent = true;
                if (Array.isArray(hop.recentIns)) {
                    for (const x of hop.recentIns) {
                        if (x && typeof x === 'object' && x.f && inNet(x.f)) { viaRecent = true; break; }
                    }
                }
            }
        }
        return { affiliated: direct || viaParent || viaRecent, direct, viaParent, viaRecent, netSize: net.size };
    }

    /**
     * КРИЕЙТОР: полный трейс С recentIns (lastFund) — сайзинг-сигналы живут здесь.
     * Эквивалент батча из одного «первого» элемента; удобен для одиночных вызовов.
     */
    traceCreator(mint, t0, wallet, per = {}) {
        return this.trace(wallet, t0, mint,
            { priority: true, ...per, wantRecent: this.cfg.recentInsMode !== 'none' });
    }

    /**
     * ДЕРЖАТЕЛЬ/ПОКУПАТЕЛЬ: трейс БЕЗ recentIns (дёшево, ~3-6 RPC на новый кошелёк).
     * Используй для buyer4..buyer6, приходящих по одному после первого батча.
     */
    traceBuyer(mint, t0, wallet, per = {}) {
        return this.trace(wallet, t0, mint, { ...per,
            wantRecent: this.cfg.recentInsMode === 'all',
            riLookup: this.cfg.buyerLastLookup });        // байерам короткое окно recentIns
    }

    /**
     * Батч: криейтор + первые держатели минта, с ограниченным параллелизмом.
     * @param {string} mint
     * @param {number} t0
     * @param {string[]} wallets — например [creator, ...top6holders]
     * @returns {Promise<object[]>} результаты в исходном порядке
     */
    async traceHolders(mint, t0, wallets) {
        const uniq = [...new Set(wallets)];
        const results = new Array(uniq.length);
        let i = 0;
        const worker = async () => {
            while (i < uniq.length) {
                const my = i++;
                const wantRecent = this.cfg.recentInsMode === 'all'
                    || (this.cfg.recentInsMode === 'creator-only' && my === 0);
                try { results[my] = await this.trace(uniq[my], t0, mint, { wantRecent }); }
                catch (e) {
                    results[my] = { mint, wallet: uniq[my], t0, error: String(e.message || e) };
                    this._emit(results[my]);
                }
            }
        };
        await Promise.all([...Array(Math.min(this.cfg.concurrency, uniq.length))].map(worker));
        return results;
    }

    /** Статистика инстанса. */
    stats() {
        return { rpcCalls: this.rpcCalls, rpcRequests: this.rpcRequests, rpc429: this.rpc429,
            rpcCallsPerKey: [...this.rpcCallsPerKey],
            keyRps: this._buckets.map(b => +b.rps.toFixed(2)),
            batchSize: this._batchSize,
            activeTraces: this._tracesActive, queuedTraces: this._traceQueue.length,
            activeRpc: this._rpcActive, queuedRpc: this._rpcQueue.length,
            cacheSize: this.hopCache.size, nodeIndex: this.nodeIndex.size };
    }

    /** Корректное закрытие стримов (при останове trader.js). */
    async close() {
        await new Promise(r => this.cacheOut.end(r));
        if (this.resultOut) await new Promise(r => this.resultOut.end(r));
    }

    /** =============== ВНУТРЕННЕЕ =============== */

    _loadCache() {
        if (!fs.existsSync(this.cfg.cacheFile)) return;
        for (const line of fs.readFileSync(this.cfg.cacheFile, 'utf8').split('\n')) {
            if (!line.trim()) continue;
            try {
                const o = JSON.parse(line);
                const prev = this.hopCache.get(o.a);
                // разрешение дублей (параллельные прогоны): parent > aged > none; ранний firstTs
                if (prev && this._score(prev) >= this._score(o.r)) continue;
                this.hopCache.set(o.a, o.r);
            } catch (e) { /* битые строки игнорируем */ }
        }
    }

    _score(r) {
        const t = r.parent !== undefined ? 2 : (r.aged ? 1 : 0);
        return t * 1e15 + (r.firstTs ? (1e15 - r.firstTs) / 1e6 : 0);
    }

    _cachePut(addr, r) {
        if (!r.at) r.at = Date.now();          // время снимка (для TTL скоропортящихся полей)
        this.hopCache.set(addr, r);
        this.cacheOut.write(JSON.stringify({ a: addr, r }) + '\n');
    }

    /** Выбрасывает из окна вызовы старше секунды. */
    _trimWindow(b, now) {
        while (b.hits.length && b.hits[0].t <= now - 1000) {
            b.used -= b.hits[0].w;
            b.hits.shift();
        }
        if (b.hits.length === 0) b.used = 0;
    }

    /**
     * Пытается занять место в окне ключа под weight вызовов.
     *
     * @returns {number} 0 — место занято, иначе через сколько мс пробовать снова
     */
    _tryReserve(i, weight, now) {
        const b = this._buckets[i];
        if (b.cooldownUntil > now) return b.cooldownUntil - now;
        this._trimWindow(b, now);
        const limit = Math.max(1, Math.floor(b.rps));
        /** Равномерный шаг внутри окна. Одного окна мало: Helius режет и «плотную»
         *  пачку, уложившуюся в лимит за первые 100 мс секунды, — 429 приходили
         *  ровно на границе окна. Поэтому вызовы ключа разносим на 1000/limit мс. */
        const gapWait = b.lastAt + (1000 / limit) * weight - now;
        if (gapWait > 0) return Math.ceil(gapWait);
        /** used === 0: запрос тяжелее секундного бюджета пускаем на пустое окно */
        if (b.used + weight <= limit || b.used === 0) {
            b.hits.push({ t: now, w: weight });
            b.used += weight;
            b.lastAt = now;
            return 0;
        }
        return Math.max(5, b.hits[0].t + 1000 - now);
    }

    /** 429/5xx по ключу: режем его темп (lowerFactor) и уводим ключ в cooldown на
     *  секунду — ровно на длину окна (Retry-After уважается). Прежде наказания не
     *  было вовсе: ретрай мгновенно уходил на соседний ключ, а перегруженный ключ
     *  тут же выбирался снова, и пул продолжал стучаться в потолок. */
    _penalizeKey(i, retryAfter) {
        const b = this._buckets[i];
        b.rps = Math.max(this.cfg.minPerKeyRps, b.rps * this.cfg.lowerFactor);
        b.successes = 0;
        const seconds = retryAfter ? parseInt(retryAfter, 10) : NaN;
        /** Окно уже «испорчено»: ждём, пока оно полностью съедет */
        const waitMs = Number.isFinite(seconds) && seconds > 0
            ? Math.min(10000, seconds * 1000)
            : 1000;
        b.cooldownUntil = Date.now() + waitMs;
        return waitMs;
    }

    /** Успех: после серии удачных запросов темп ключа растёт обратно к perKeyRps */
    _rewardKey(i) {
        const b = this._buckets[i];
        if (b.rps >= this.cfg.perKeyRps) return;
        if (++b.successes >= this.cfg.raiseAfter) {
            b.successes = 0;
            b.rps = Math.min(this.cfg.perKeyRps, b.rps + this.cfg.raiseStep);
        }
    }

    /** Выбор ключа: без perKeyRps — слепой round-robin; с ним — первый остывший ключ,
     *  у которого в секундном окне есть место под weight вызовов. Если места нет ни
     *  у кого — ждём ближайшее освобождение вместо ловли 429.
     *  @returns {Promise<{url: string, index: number}>} */
    async _nextUrl(weight = 1) {
        if (!this.cfg.perKeyRps) {
            const i = this._rrIdx;
            this._rrIdx = (this._rrIdx + 1) % this.rpcUrls.length;
            this.rpcCallsPerKey[i] += weight;
            return { url: this.rpcUrls[i], index: i };
        }
        for (;;) {
            let bestWait = Infinity;
            const now = Date.now();
            for (let k = 0; k < this.rpcUrls.length; k++) {
                const i = (this._rrIdx + k) % this.rpcUrls.length;
                const wait = this._tryReserve(i, weight, now);
                if (wait === 0) {
                    this._rrIdx = (i + 1) % this.rpcUrls.length;
                    this.rpcCallsPerKey[i] += weight;
                    return { url: this.rpcUrls[i], index: i };
                }
                bestWait = Math.min(bestWait, wait);
            }
            await sleep(Math.max(5, Math.ceil(bestWait)));
        }
    }

    /** Глобальный шейпер одновременных HTTP-запросов (защита ключей). */
    async _withRpcSlot(fn) {
        if (this._rpcActive >= this.cfg.maxConcurrentRpc) {
            await new Promise(res => this._rpcQueue.push(res));
        }
        this._rpcActive++;
        try {
            return await fn();
        } finally {
            this._rpcActive--;
            const next = this._rpcQueue.shift();
            if (next) next();
        }
    }

    async _rpc(method, params) {
        return this._withRpcSlot(() => this._send({ jsonrpc: '2.0', id: 1, method, params }))
            .then(j => {
                if (j.error) throw new Error(j.error.message);
                return j.result;
            });
    }

    /**
     * Пачка однотипных вызовов (десятки getTransaction на кошелёк — самая дорогая
     * часть трейса). При batchSize > 1 уходит одним JSON-RPC batch-запросом, при
     * batchSize = 1 (по умолчанию) — обычными одиночными вызовами через тот же
     * ограничитель. Замер показал, что на free Helius батч НЕ экономит лимит:
     * см. комментарий к batchSize в DEFAULTS.
     *
     * @param {Array<{method: string, params: Array}>} calls
     * @returns {Promise<Array<*|null>>} результаты в порядке calls (null — не получилось)
     */
    async _rpcBatch(calls) {
        if (calls.length === 0) return [];
        /** batchSize 1 — честные одиночные запросы (см. комментарий к batchSize) */
        if (this._batchSize <= 1 || calls.length === 1) {
            return Promise.all(calls.map(c => this._rpc(c.method, c.params).catch(() => null)));
        }

        const out = new Array(calls.length).fill(null);
        const chunks = [];
        for (let i = 0; i < calls.length; i += this._batchSize) {
            chunks.push(calls.slice(i, i + this._batchSize).map((c, k) => ({ call: c, at: i + k })));
        }

        await Promise.all(chunks.map(chunk => this._withRpcSlot(async () => {
            /** Позиции, которые ещё предстоит получить: сабзапрос может вернуть
             *  собственный rate-error внутри 200 OK — такие повторяем пачкой. */
            let pending = chunk;
            for (let attempt = 0; attempt < this.cfg.retries && pending.length; attempt++) {
                const payload = pending.map((p, id) => ({
                    jsonrpc: '2.0', id, method: p.call.method, params: p.call.params,
                }));
                let response;
                try {
                    response = await this._send(payload, pending.length);
                } catch (e) {
                    return;
                }
                const items = Array.isArray(response) ? response : [response];
                const byId = new Map(items.map(it => [it.id, it]));
                const retry = [];
                for (let id = 0; id < pending.length; id++) {
                    const item = byId.get(id);
                    if (item && item.error) {
                        if (String(item.error.message || '').match(/rate|too many/i)) {
                            retry.push(pending[id]);
                        }
                        continue;
                    }
                    if (item) out[pending[id].at] = item.result;
                }
                pending = retry;
                if (pending.length) await sleep(300 * Math.pow(2, attempt));
            }
        })));

        return out;
    }

    /**
     * Один HTTP-запрос к Helius с ретраями, ротацией ключей и адаптивным темпом.
     *
     * @param {object|Array} payload одиночный JSON-RPC объект или батч
     * @param {number} weight сколько вызовов внутри (для статистики)
     * @returns {Promise<object|Array>} разобранное тело ответа
     */
    async _send(payload, weight = 1) {
        let lastErr = null;
        for (let attempt = 0; attempt < this.cfg.retries; attempt++) {
            const { url, index } = await this._nextUrl(weight);
            this.rpcCalls += weight;
            this.rpcRequests++;
            try {
                const res = await fetch(url, {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify(payload),
                    signal: AbortSignal.timeout(this.cfg.rpcTimeoutMs),
                });
                if (res.status === 429 || res.status >= 500) {
                    if (res.status === 429) this.rpc429++;
                    const wait = this._penalizeKey(index, res.headers.get('retry-after'));
                    lastErr = new Error(`HTTP ${res.status}`);
                    /** С несколькими ключами ждать не нужно: _nextUrl сам выберет
                     *  остывший ключ, а наказанный выпал из выбора. */
                    if (this.rpcUrls.length === 1) await sleep(wait);
                    continue;
                }
                const j = await res.json();
                /** Rate-error может приехать и внутри 200 OK — это тоже 429 по смыслу */
                if (!Array.isArray(j) && j.error && String(j.error.message || '').match(/rate|too many/i)) {
                    this.rpc429++;
                    this._penalizeKey(index, null);
                    lastErr = new Error(j.error.message);
                    continue;
                }
                this._rewardKey(index);
                return j;
            } catch (e) {
                lastErr = e;
                this._penalizeKey(index, null);
                await sleep(300 * Math.pow(2, attempt));
            }
        }
        throw lastErr || new Error('rpc failed');
    }

    _extractIncoming(tx, address) {
        const instrs = [
            ...(tx.transaction?.message?.instructions || []),
            ...((tx.meta?.innerInstructions || []).flatMap(x => x.instructions || [])),
        ];
        for (const ins of instrs) {
            const p = ins.parsed;
            if (!p || ins.program !== 'system') continue;
            if ((p.type === 'transfer' || p.type === 'transferWithSeed' || p.type === 'createAccount')
                && (p.info?.destination === address || p.info?.newAccount === address)) {
                const from = p.info?.source || p.info?.base || null;
                const lamports = Number(p.info?.lamports || 0);
                if (from && from !== address && lamports > 0) {
                    return { from, amount: lamports / 1e9, ts: (tx.blockTime || 0) * 1000 };
                }
            }
        }
        return null;
    }

    async _recentIncomings(address, sigs, lookup = null) {
        /** recentIns — самая дорогая часть трейса: до lookup getTransaction на кошелёк,
         *  ~90% всех RPC-вызовов. Тянем НЕ все кандидаты разом, а порциями riChunk и
         *  останавливаемся, как только набрали lastKeep входящих: у свежего кошелька
         *  зарядка лежит в первых же транзакциях, и хвост окна тянуть незачем.
         *  Порядок (новейшие первыми) и семантика «первые lastKeep входящих»
         *  сохранены — порции идут последовательно. Внутри порции — параллельно,
         *  поэтому по времени это riChunk-кратное число RTT, не lookup-кратное. */
        const cand = sigs.slice(0, lookup || this.cfg.lastLookup).filter(s => !s.err);
        const chunk = Math.max(1, this.cfg.riChunk || cand.length);
        const out = [];
        for (let i = 0; i < cand.length && out.length < this.cfg.lastKeep; i += chunk) {
            const txs = await this._rpcBatch(cand.slice(i, i + chunk).map(s => ({
                method: 'getTransaction',
                params: [s.signature, { maxSupportedTransactionVersion: 0, encoding: 'jsonParsed' }],
            })));
            for (const tx of txs) {
                if (out.length >= this.cfg.lastKeep) break;
                if (!tx) continue;
                const inc = this._extractIncoming(tx, address);
                if (inc) out.push({ t: inc.ts, f: inc.from });
            }
        }
        return out;
    }

    /** Один хоп с кэшем и inflight-дедупликацией (общие родители не трейсятся дважды). */
    _firstIncoming(address, wantRecent = true, lookup = null) {
        const cached = this.hopCache.get(address);
        if (cached) {
            const age = Date.now() - (cached.at || 0);
            /** none-запись: кошелёк был пуст на момент снимка, но мог быть заряжен
             *  после — старше TTL перепроверяем полностью (единственный случай,
             *  когда инвариантная часть записи может ПОЯВИТЬСЯ). */
            if (cached.none && age > this.cfg.noneTtlMs) {
                return this._dedupInflight(address, () => this._firstIncomingRaw(address, wantRecent, lookup));
            }
            /** recentIns — снимок: свежая зарядка после снимка невидима.
             *  Нужен свежий recentIns (wantRecent) и снимок старше TTL — дешёвое
             *  освежение: 1 страница новейших сигнатур, parent/firstTs из кэша. */
            if (wantRecent && !cached.none) {
                if (!Array.isArray(cached.recentIns) || age > this.cfg.recentTtlMs) {
                    return this._dedupInflight(address + '#r', () => this._refreshRecent(address, cached, lookup));
                }
            }
            return Promise.resolve(cached);
        }
        return this._dedupInflight(address, () => this._firstIncomingRaw(address, wantRecent, lookup));
    }

    _dedupInflight(key, fn) {
        if (this.inflight.has(key)) return this.inflight.get(key);
        const p = fn().finally(() => this.inflight.delete(key));
        this.inflight.set(key, p);
        return p;
    }

    /** Дешёвое освежение recentIns существующей записи: одна страница новейших
     *  сигнатур + до lastLookup tx. Инвариантные поля (parent/firstTs/amount) — из кэша. */
    async _refreshRecent(address, cached, lookup = null) {
        const sigs = await this._rpc('getSignaturesForAddress',
            [address, { limit: this.cfg.sigPageLimit }]);
        if (!sigs || sigs.length === 0) return cached;
        const recentIns = await this._recentIncomings(address, sigs, lookup);
        const r = { ...cached, recentIns, sigCount: Math.max(cached.sigCount || 0, sigs.length), at: Date.now() };
        this._cachePut(address, r);
        return r;
    }

    async _firstIncomingRaw(address, wantRecent = true, lookup = null) {
        let sigs = [];
        let before = undefined;
        for (let page = 0; page < this.cfg.maxSigPages; page++) {
            const batch = await this._rpc('getSignaturesForAddress',
                [address, { limit: this.cfg.sigPageLimit, before }]);
            if (!batch || batch.length === 0) break;
            sigs = sigs.concat(batch);
            if (batch.length < this.cfg.sigPageLimit) break;
            before = batch[batch.length - 1].signature;
            if (sigs.length >= this.cfg.agedSigs) break;
        }
        if (sigs.length === 0) {
            const r = { none: true, sigCount: 0, firstTs: null, recentIns: [] };
            this._cachePut(address, r); return r;
        }
        const sigCount = sigs.length;
        const earliest = sigs.slice(-this.cfg.txLookup).reverse();
        const firstTs = (earliest[0].blockTime || 0) * 1000;
        // recentIns: null = «не собирали» (кэш-запись честна о своей неполноте);
        // [] = «собирали, входящих не нашли». lastFund у null-записей будет null.
        const recentIns = wantRecent ? await this._recentIncomings(address, sigs, lookup) : null;

        if (sigCount >= this.cfg.agedSigs) {
            const r = { aged: true, sigCount, firstTs, recentIns };
            this._cachePut(address, r); return r;
        }
        {
            const cand = earliest.filter(s => !s.err);
            const txs = await this._rpcBatch(cand.map(s => ({
                method: 'getTransaction',
                params: [s.signature, { maxSupportedTransactionVersion: 0, encoding: 'jsonParsed' }],
            })));
            for (const tx of txs) {                       // порядок: от самой ранней
                if (!tx) continue;
                const inc = this._extractIncoming(tx, address);
                if (inc) {
                    const r = { parent: inc.from, amount: inc.amount, firstTs: inc.ts, sigCount, recentIns };
                    this._cachePut(address, r); return r;
                }
            }
        }
        const r = { none: true, sigCount, firstTs, recentIns };
        this._cachePut(address, r); return r;
    }

    async _traceFunding(startWallet, mintTs, cfg) {
        const chain = [];
        let addr = startWallet;
        let clusterHit = false;
        let terminalType = 'depth_limit';
        let minAge = Infinity;
        let lastFundAgeSec = null;
        let lastFundCensored = false;

        for (let depth = 0; depth < cfg.maxDepth; depth++) {
            // recentIns нужен только стартовому кошельку (lastFund) — родителям никогда
            const hop = await this._firstIncoming(addr,
                depth === 0 && cfg.wantRecent !== false, cfg.riLookup || null);

            if (depth === 0) {
                if (Array.isArray(hop.recentIns)) {
                    const tsOf = x => (typeof x === 'number' ? x : x.t);   // совместимость со старым кэшем
                    const before = hop.recentIns.map(tsOf).filter(ts => ts <= mintTs);
                    if (before.length > 0) lastFundAgeSec = Math.round((mintTs - Math.max(...before)) / 1000);
                    else if (hop.recentIns.length >= cfg.lastKeep) lastFundCensored = true;
                    else if (hop.firstTs > 0 && hop.firstTs <= mintTs) {
                        /** recentIns собирали и входящих не нашли (у fat-ботов новейшие tx —
                         *  свопы): последняя ИЗВЕСТНАЯ зарядка = самая ранняя видимая tx.
                         *  Для fat это граница листания — НИЖНЯЯ оценка возраста зарядки. */
                        lastFundAgeSec = Math.round((mintTs - hop.firstTs) / 1000);
                    } else lastFundCensored = true;
                } else {
                    /** recentIns не собирался (legacy-кэш или байер-режим) — честно
                     *  помечаем цензуру, а не молчаливый null. */
                    lastFundCensored = true;
                }
            }

            if (hop.none) { terminalType = 'dead_end'; break; }
            if (hop.aged) { terminalType = depth === 0 ? 'creator_fat' : 'fat'; break; }

            const ageSec = hop.firstTs ? Math.max(0, (mintTs - hop.firstTs) / 1000) : null;
            if (ageSec !== null && ageSec > cfg.agedDays * 86400) {
                terminalType = depth === 0 ? 'creator_aged' : 'aged';
                break;
            }
            if (ageSec !== null && ageSec < minAge) minAge = ageSec;

            chain.push({
                wallet: addr,
                parent: hop.parent,
                amount: +hop.amount.toFixed(4),
                walletAgeSec: ageSec === null ? null : Math.round(ageSec),
                sigCount: hop.sigCount,
            });

            if (KNOWN_CEX.has(hop.parent)) { terminalType = 'cex'; break; }
            if (this.nodeIndex.has(hop.parent)) { terminalType = 'cluster'; clusterHit = true; break; }
            addr = hop.parent;
        }

        for (const h of chain) { this.nodeIndex.add(h.wallet); this.nodeIndex.add(h.parent); }

        return {
            fundingDepth: chain.length,
            terminalType,
            creatorAgeSec: chain.length ? chain[0].walletAgeSec : null,
            lastFundAgeSec,
            lastFundCensored,
            chainMinAgeSec: Number.isFinite(minAge) ? Math.round(minAge) : null,
            fundingAmount: chain.length ? chain[0].amount : null,
            clusterHit,
            chain,
        };
    }
}

module.exports = FundingTracer;