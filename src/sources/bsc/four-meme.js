/**
 * Four.meme (BSC, TokenManager2): создания токенов и сделки кривой → four_meme_bc_tokens / four_meme_bc_swaps,
 * каналы bsc_four_meme (сделка или {removed}) и bsc_four_meme_token. Перенесено из node/bsc/four-meme-collector.js.
 */
const {getLogger, rpcCall, word, address, string, units, meta, createWriter, subscribeLogs} = require('./common');

const logger = getLogger('four-meme');

const TOKEN_MANAGER2 = '0x5c952063c7fc8610ffdb798152d69f0b9550762b';

/** keccak256 сигнатур событий */
const TOPIC_TOKEN_CREATE = '0x396d5e902b675b032348d3d2e9517ee8f0c4a926603fbc075d3d282ff00cad20'; // TokenCreate(address,address,uint256,string,string,uint256,uint256,uint256)
const TOPIC_TOKEN_PURCHASE = '0x7db52723a3b2cdd6164364b3b766e65e540d7be48ffa89582956d8eaebe62942'; // TokenPurchase(address,address,uint256,uint256,uint256,uint256,uint256,uint256)
const TOPIC_TOKEN_SALE = '0x0a5575b3648bae2210cee56bf33254cc1ddfbc7bf637c0af2ac18b14fb1bae19'; // TokenSale(address,address,uint256,uint256,uint256,uint256,uint256,uint256)

/** селектор _tokenInfos(address) */
const TOKEN_INFOS_SELECTOR = '0xe684626b';

const SWAP_COLUMNS = ['type', 'token', 'account', 'price', 'amount', 'cost', 'fee', 'offers', 'funds'];
const TOKEN_COLUMNS = ['creator', 'token', 'requestId', 'name', 'symbol', 'totalSupply', 'launchTime', 'launchFee', 'quote'];
const META = ['tx_hash', 'log_index', 'block', 'block_timestamp', 'received_at'];
const obj = (cols, row) => Object.fromEntries([...cols, ...META].map((c, i) => [c, row[i]]));

function startFourMeme({db, write, publish}) {
    const stats = {logs: 0, swaps: 0, tokens: 0, removed: 0, errors: 0, reconnects: 0};
    const swaps = createWriter(db, write, 'four_meme_bc_swaps', SWAP_COLUMNS, logger);
    const tokens = createWriter(db, write, 'four_meme_bc_tokens', TOKEN_COLUMNS, logger);

    async function getQuote(token) {
        const data = TOKEN_INFOS_SELECTOR + token.slice(2).padStart(64, '0');
        const result = await rpcCall('eth_call', [{to: TOKEN_MANAGER2, data}, 'latest']);
        return address(result.slice(2), 1);
    }

    function onSwap(log, receivedAt) {
        const hex = log.data.slice(2);
        const row = [
            log.topics[0] === TOPIC_TOKEN_PURCHASE ? 'TokenPurchase' : 'TokenSale',
            address(hex, 0), // token
            address(hex, 1), // account
            units(word(hex, 2)), // price
            units(word(hex, 3)), // amount
            units(word(hex, 4)), // cost
            units(word(hex, 5)), // fee
            units(word(hex, 6)), // offers
            units(word(hex, 7)), // funds
            ...meta(log, receivedAt),
        ];
        swaps.push(row); stats.swaps++;
        publish('bsc_four_meme', obj(SWAP_COLUMNS, row));
    }

    async function onTokenCreate(log, receivedAt) {
        const hex = log.data.slice(2);
        const token = address(hex, 1);
        let quote = null;
        try { quote = await getQuote(token); } catch (e) { logger.error(`quote для ${token}: ${e.message}`); }
        const row = [
            address(hex, 0), // creator
            token,
            word(hex, 2).toString(), // requestId
            string(hex, Number(word(hex, 3))), // name
            string(hex, Number(word(hex, 4))), // symbol
            units(word(hex, 5)), // totalSupply
            word(hex, 6).toString(), // launchTime
            units(word(hex, 7)), // launchFee
            quote,
            ...meta(log, receivedAt),
        ];
        tokens.push(row); stats.tokens++;
        publish('bsc_four_meme_token', obj(TOKEN_COLUMNS, row));
    }

    function onLog(log, receivedAt) {
        if (log.removed) {
            logger.warn(`отменён лог ${log.transactionHash}:${log.logIndex}`);
            stats.removed++;
            const isToken = log.topics[0] === TOPIC_TOKEN_CREATE, logIndex = parseInt(log.logIndex, 16);
            (isToken ? tokens : swaps).remove(log.transactionHash, logIndex);
            return publish(isToken ? 'bsc_four_meme_token' : 'bsc_four_meme', {removed: true, tx_hash: log.transactionHash, log_index: logIndex});
        }
        switch (log.topics[0]) {
            case TOPIC_TOKEN_PURCHASE:
            case TOPIC_TOKEN_SALE:
                return onSwap(log, receivedAt);
            case TOPIC_TOKEN_CREATE:
                return onTokenCreate(log, receivedAt).catch(e => { stats.errors++; logger.error(e.message); });
        }
    }

    const sub = subscribeLogs({address: TOKEN_MANAGER2, topics: [TOPIC_TOKEN_CREATE, TOPIC_TOKEN_PURCHASE, TOPIC_TOKEN_SALE], onLog, logger, stats});
    return {stats, stop: () => { sub.stop(); return Promise.all([swaps.close(), tokens.close()]); }};
}

module.exports = {startFourMeme};
