# sol-collector-hub

Единая точка между Triton (Yellowstone gRPC) и нашими ботами на Solana:

- **одна подписка** Triton на все площадки, которые мы собираем (pump.fun bonding curve, pump.fun AMM, Raydium LaunchLab, Meteora DBC);
- **разбор** сделок, пулов и событий;
- **сбор в MySQL** — те же таблицы и строки, что писали коллекторы и трейдер в `copy-trade-solana` (выгрузки в parquet не меняются);
- **раздача** разобранного ботам по TCP: бот сам выбирает каналы.

Торговой логики здесь нет. Боты решают и торгуют сами, хаб только принимает поток, разбирает, пишет и раздаёт.

## Источники и каналы

| Канал | Что приходит | Откуда (перенесено) |
|---|---|---|
| `pump_bc` | одна запись на транзакцию pump.fun кривой: `{sig, slot, ts, recvTs, cuPrice, cuLimit, signers, tips, create, tokenProgram, createData, tokens, swaps: [...]}`; смена криейтора — `{sig, slot, ts, recvTs, creatorChange}` | `bot/utils.js` (разбор), `sandbox/trader.js` (запись) |
| `pump_amm` | сделка PumpSwap в пулах, мигрировавших с кривой, первые 60 мин | `collectors/pump-amm.js`, `collectors/amm-run.js` |
| `pump_amm_pool` | новый пул миграции | там же |
| `launchlab`, `launchlab_pool` | сделки и пулы Raydium LaunchLab | `collectors/launchlab.js` |
| `meteora_dbc`, `meteora_dbc_pool`, `meteora_dbc_complete` | сделки, пулы, завершения кривой Meteora DBC | `collectors/meteora-dbc.js` |
| `blockhash` | свежий blockhash `{hash, slot, lastValidBlockHeight, ts}` (confirmed, опрос RPC раз в 1 с); при подключении — последние 30 | `services/blockhash.js` |

`ts` — время из Triton (`createdAt`), мс; `recvTs` — время прихода на хаб. Суммы и резервы в `pump_bc` — целые `BigInt`, как в событии.

### Что пишется в MySQL (`DB_WRITE=1`)

- `pump_fun_bc_swaps`, `pump_fun_bc_mints` — правила трейдера: SOL-кривые 185 с после создания монеты, mayhem — 10 мин, кривые не в SOL — все сделки;
- `lp_pump_amm_swaps/pools`, `lp_launchlab_swaps/pools`, `lp_meteora_dbc_swaps/pools/completes` — как у `collectors/`.

Таблицы создаются при старте (`schema.sql`, `CREATE TABLE IF NOT EXISTS`).

## Протокол раздачи

TCP, `TCP_NODELAY`, одно соединение на бота.

1. Бот → хаб, одна строка JSON:
   `{"hello": 1, "name": "micro-02-low", "channels": ["pump_bc"], "resume": {"pump_bc": 123456}}`
   `resume` — последний полученный номер. Хаб досылает пропущенное из кольца (`HUB_RING` записей на канал); если номера в кольце уже нет — служебный кадр `gap`.
2. Хаб → бот: кадры `[u32 LE длина][u8 тип][u8 канал][u64 LE номер][тело msgpack]`.
   Тип 1 — данные, 2 — служебный (`welcome`, `gap`, `ping` раз в 5 с).

Отставший клиент (буфер сокета больше `HUB_MAX_CLIENT_BUF_MB`) отключается, остальных он не тормозит.

Клиент для ботов — `client/hub-client.js` (переподключение, дозапрос, контроль номеров, события `down`/`up` при тишине хаба). Боты Triton не слушают — только хаб.

### Фильтры подписки (`subscribe`)

```json
{"pump": {"bc": true, "amm": true, "mayhem": false, "quotes": ["SOL", "pumpCm…"], "creatorChange": true, "pools": true},
 "launchlab": {"quotes": [...], "platforms": [...], "pools": true},
 "meteora_dbc": {"quotes": [...], "configs": [...], "pools": true, "completes": true}}
```

`mayhem`: `true` — включать (по умолчанию), `false` — исключать, `"only"` — только mayhem. `quotes` — контракты валют котировки, SOL — `"SOL"`. Нет ключа — без ограничения. Подробно — `src/filters.js`.

## Запуск

```
cp .env.example .env    # заполнить Triton, RPC, MySQL
npm install
node src/index.js
```

Переменные — `.env.example`. Проверочный клиент: `node scripts/listen.js <host> <port> pump_bc 30`.

Раз в минуту в лог — `{hub: …}`: задержка Triton → хаб (p50/p90/p99), CPU, задержка event loop, отдано записей и МБ, клиенты, счётчики по площадкам.
