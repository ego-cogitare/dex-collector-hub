/**
 * Flap.sh (BSC, Portal): создания токенов и сделки кривой → flap_bc_tokens / flap_bc_swaps,
 * каналы bsc_flap (сделка или {removed}) и bsc_flap_token. Перенесено из node/bsc/flap-collector.js.
 */
const {getLogger, word, address, string, units, meta, createWriter, subscribeLogs} = require('./common');

const logger = getLogger('flap');

const PORTAL = '0xe2ce6ab80874fa9fa2aae65d277dd6b8e65c9de0';

/** keccak256 сигнатур событий */
const TOPIC_TOKEN_CREATED = '0x504e7f360b2e5fe33cbaaae4c593bc55305328341bf79009e43e0e3b7f699603'; // TokenCreated(uint256,address,uint256,address,string,string,string)
const TOPIC_TOKEN_BOUGHT = '0xa800a2038683844fac66747f771bfdfae862eb28b16bcfa387afa9fbacce8ff7'; // TokenBought(uint256,address,address,uint256,uint256,uint256,uint256)
const TOPIC_TOKEN_SOLD = '0x03a4693e592f5e75dc7c136acb39b146d2b4966c0e509c34f362dee02b3b861a'; // TokenSold(uint256,address,address,uint256,uint256,uint256,uint256)
const TOPIC_TOKEN_QUOTE_SET = '0x3ceb902d3c555c21c3415b6aa839104b18e4825b2f8324011ff979089a507a8c'; // TokenQuoteSet(address,address)
const TOPIC_TAX_SET = '0x1546924f4680b1b2e093fd251c437c8781d81a21ed8ad1895e1f2c9b78db0cd0'; // FlapTokenTaxSet(address,uint256)
const TOPIC_ASYMMETRIC_TAX_SET = '0x46cc246a238d1ca0951a15200994903e2d56cbb0389e63f09d66412a787aa3c0'; // FlapTokenAsymmetricTaxSet(address,uint256,uint256)

/** Котировка и налоги приходят следом за TokenCreated в той же транзакции — ждём их, потом пишем токен */
const TOKEN_ASSEMBLE_DELAY = 3000;

const SWAP_COLUMNS = ['type', 'ts', 'token', 'buyer', 'seller', 'amount', 'eth', 'fee', 'postPrice'];
const TOKEN_COLUMNS = ['ts', 'creator', 'nonce', 'token', 'name', 'symbol', 'meta', 'quoteToken', 'tax', 'buyTax', 'sellTax'];
const META = ['tx_hash', 'log_index', 'block', 'block_timestamp', 'received_at'];
const obj = (cols, row) => Object.fromEntries([...cols, ...META].map((c, i) => [c, row[i]]));

function startFlap({db, write, publish}) {
    const stats = {logs: 0, swaps: 0, tokens: 0, removed: 0, errors: 0, reconnects: 0};
    const swaps = createWriter(db, write, 'flap_bc_swaps', SWAP_COLUMNS, logger);
    const tokens = createWriter(db, write, 'flap_bc_tokens', TOKEN_COLUMNS, logger);

    /** адрес токена → собираемая строка токена */
    const pending = {};

    function onSwap(log, receivedAt) {
        const hex = log.data.slice(2);
        const isBuy = log.topics[0] === TOPIC_TOKEN_BOUGHT;
        const trader = address(hex, 2);
        const row = [
            isBuy ? 'TokenBought' : 'TokenSold',
            word(hex, 0).toString(), // ts
            address(hex, 1), // token
            isBuy ? trader : null, // buyer
            isBuy ? null : trader, // seller
            units(word(hex, 3)), // amount
            units(word(hex, 4)), // eth
            units(word(hex, 5)), // fee
            units(word(hex, 6)), // postPrice
            ...meta(log, receivedAt),
        ];
        swaps.push(row); stats.swaps++;
        publish('bsc_flap', obj(SWAP_COLUMNS, row));
    }

    function onTokenCreated(log, receivedAt) {
        const hex = log.data.slice(2);
        const token = address(hex, 3);
        pending[token] = {
            ts: word(hex, 0).toString(),
            creator: address(hex, 1),
            nonce: word(hex, 2).toString(),
            token,
            name: string(hex, Number(word(hex, 4))),
            symbol: string(hex, Number(word(hex, 5))),
            meta: string(hex, Number(word(hex, 6)), 1024),
            quoteToken: null,
            tax: null,
            buyTax: null,
            sellTax: null,
            log: meta(log, receivedAt),
        };
        setTimeout(() => {
            const t = pending[token];
            delete pending[token];
            const row = [t.ts, t.creator, t.nonce, t.token, t.name, t.symbol, t.meta, t.quoteToken, t.tax, t.buyTax, t.sellTax, ...t.log];
            tokens.push(row); stats.tokens++;
            publish('bsc_flap_token', obj(TOKEN_COLUMNS, row));
        }, TOKEN_ASSEMBLE_DELAY);
    }

    /** Настройки токена: TokenQuoteSet, FlapTokenTaxSet, FlapTokenAsymmetricTaxSet */
    function onTokenSetting(log) {
        const hex = log.data.slice(2);
        const t = pending[address(hex, 0)];
        if (!t) return;
        switch (log.topics[0]) {
            case TOPIC_TOKEN_QUOTE_SET: t.quoteToken = address(hex, 1); break;
            case TOPIC_TAX_SET: t.tax = word(hex, 1).toString(); break;
            case TOPIC_ASYMMETRIC_TAX_SET: t.buyTax = word(hex, 1).toString(); t.sellTax = word(hex, 2).toString(); break;
        }
    }

    function onLog(log, receivedAt) {
        if (log.removed) {
            if (log.topics[0] === TOPIC_TOKEN_CREATED || log.topics[0] === TOPIC_TOKEN_BOUGHT || log.topics[0] === TOPIC_TOKEN_SOLD) {
                logger.warn(`отменён лог ${log.transactionHash}:${log.logIndex}`);
                stats.removed++;
                const isToken = log.topics[0] === TOPIC_TOKEN_CREATED, logIndex = parseInt(log.logIndex, 16);
                (isToken ? tokens : swaps).remove(log.transactionHash, logIndex);
                publish(isToken ? 'bsc_flap_token' : 'bsc_flap', {removed: true, tx_hash: log.transactionHash, log_index: logIndex});
            }
            return;
        }
        switch (log.topics[0]) {
            case TOPIC_TOKEN_BOUGHT:
            case TOPIC_TOKEN_SOLD:
                return onSwap(log, receivedAt);
            case TOPIC_TOKEN_CREATED:
                return onTokenCreated(log, receivedAt);
            default:
                return onTokenSetting(log);
        }
    }

    const sub = subscribeLogs({address: PORTAL, topics: [TOPIC_TOKEN_CREATED, TOPIC_TOKEN_BOUGHT, TOPIC_TOKEN_SOLD, TOPIC_TOKEN_QUOTE_SET, TOPIC_TAX_SET, TOPIC_ASYMMETRIC_TAX_SET],
        onLog, logger, stats});
    return {stats, stop: () => { sub.stop(); return Promise.all([swaps.close(), tokens.close()]); }};
}

module.exports = {startFlap};
