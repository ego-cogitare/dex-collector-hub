# Таблицы BSC-коллекторов

База `dex`. Схема — `sql/schema.sql`. Пишут два процесса:

| Процесс | Площадка | Контракт | Таблицы |
|---|---|---|---|
| `four-meme-collector.js` | Four.meme | TokenManager2 `0x5c952063c7fc8610FFDB798152D69F0B9550762b` | `four_meme_bc_tokens`, `four_meme_bc_swaps` |
| `flap-collector.js` | Flap.sh | Portal `0xe2cE6ab80874Fa9Fa2aAE65D277Dd6B8e65C9De0` | `flap_bc_tokens`, `flap_bc_swaps` |

Собирается только этап бондинг-кривой. Сделки после миграции на DEX не пишутся.

## Общие правила

- **Имена колонок = имена полей события** (`TokenPurchase.cost` → колонка `cost`). Исключения перечислены в описании таблиц.
- **Суммы uint256 делятся на 1e18** и хранятся в `DECIMAL(65,18)` без потери точности. Это верно, если у токена и у котируемого актива 18 знаков. Так у всех токенов площадок, у BNB и у BNCB (проверено). Для других quote-токенов знаки не проверялись: если у quote не 18 знаков, суммы в quote надо пересчитать.
- **Адреса** — lowercase `0x…`, `char(42)`.
- **Источник — только WebSocket-подписка.** При обрыве связи или рестарте процесса события за это время теряются и не догружаются. Дыры в данных возможны.
- **Реорги:** если пришёл лог с `removed: true`, строка удаляется.

### Колонки лога (во всех четырёх таблицах)

| Колонка | Тип | Что это |
|---|---|---|
| `tx_hash` | char(66) | Хеш транзакции |
| `log_index` | int | Номер лога в блоке. Порядок событий внутри блока |
| `block` | int | Номер блока. Блоки BSC идут примерно каждые 0,45 с |
| `block_timestamp` | int | Время блока, **секунды** (unix) |
| `received_at` | bigint | Локальное время, когда коллектор получил лог, **миллисекунды** (unix). Не из блокчейна |

В таблицах свопов `(tx_hash, log_index)` уникален.

Точный порядок событий — `block`, затем `log_index`. Для «секунд от создания» точнее всего `received_at` или разница блоков × 0,45 с: `block_timestamp` слишком грубый.

---

## four_meme_bc_tokens

Одна строка на токен. Источник — событие `TokenCreate`.

| Колонка | Тип | Что это |
|---|---|---|
| `creator` | char(42) | Адрес создателя |
| `token` | char(42) | Адрес токена (unique) |
| `requestId` | decimal(65,0) | Внутренний id запроса Four.meme |
| `name`, `symbol` | varchar | Имя и тикер (utf8mb4, обрезаются до 255 символов) |
| `totalSupply` | decimal | Эмиссия в токенах, обычно 1 000 000 000 |
| `launchTime` | bigint | Поле события. В наблюдаемых данных всегда 0 |
| `launchFee` | decimal | Поле события. В наблюдаемых данных всегда 0 |
| `quote` | char(42) | **Не из события:** `TokenManager2._tokenInfos(token).quote` через один `eth_call` при создании. `0x000…0` = BNB, иначе адрес ERC-20 (например BNCB `0x4902…ec3f`). NULL — вызов не удался |

## four_meme_bc_swaps

Одна строка на сделку на кривой. Источник — события `TokenPurchase` / `TokenSale`.

| Колонка | Тип | Что это |
|---|---|---|
| `type` | enum | `TokenPurchase` — покупка, `TokenSale` — продажа |
| `token` | char(42) | Токен |
| `account` | char(42) | Адрес трейдера из события |
| `price` | decimal | Цена в quote за 1 токен |
| `amount` | decimal | Количество токенов в сделке |
| `cost` | decimal | Сумма в quote. Для покупки проверено: `cost ≈ price × amount` |
| `fee` | decimal | Комиссия в quote. В данных ≈ 1% от `cost` |
| `offers` | decimal | Сколько токенов осталось на кривой **после** сделки. Начинается с 800 000 000 |
| `funds` | decimal | Сколько quote собрано в кривой **после** сделки. Сверено с `_tokenInfos`: `offers` и `funds` совпадают с состоянием кривой |

Все суммы в валюте `quote` токена, поэтому при сравнении токенов нужен `JOIN` с `four_meme_bc_tokens`.

---

## flap_bc_tokens

Одна строка на токен. Источник — `TokenCreated` и события из той же транзакции: `TokenQuoteSet`, `FlapTokenTaxSet`, `FlapTokenAsymmetricTaxSet`. Строка пишется через ~3 с после `TokenCreated`, когда приходят все эти события.

| Колонка | Тип | Что это |
|---|---|---|
| `ts` | bigint | Поле события, время блока в секундах (= `block_timestamp`) |
| `creator` | char(42) | Адрес создателя. Часто это контракт бота или роутера, а не кошелёк |
| `nonce` | decimal(65,0) | Порядковый номер токена у Flap |
| `token` | char(42) | Адрес токена (unique) |
| `name`, `symbol` | varchar | Имя и тикер |
| `meta` | varchar(1024) | URI метаданных (обычно IPFS CID) |
| `quoteToken` | char(42) | Из `TokenQuoteSet`. `0x000…0` = BNB |
| `tax` | int | Из `FlapTokenTaxSet`, налог на кривой в **базисных пунктах** (100 = 1%, 899 = 8,99%) |
| `buyTax`, `sellTax` | int | Из `FlapTokenAsymmetricTaxSet`, налог на покупку и продажу в б.п. |

NULL в `quoteToken` или налогах — событие не пришло за 3 с.

## flap_bc_swaps

Одна строка на сделку на кривой. Источник — события `TokenBought` / `TokenSold`.

| Колонка | Тип | Что это |
|---|---|---|
| `type` | enum | `TokenBought` — покупка, `TokenSold` — продажа |
| `ts` | bigint | Поле события, время блока в секундах |
| `token` | char(42) | Токен |
| `buyer` | char(42) | Поле `buyer` из `TokenBought`. Заполнено только у покупок |
| `seller` | char(42) | Поле `seller` из `TokenSold`. Заполнено только у продаж |
| `amount` | decimal | Количество токенов |
| `eth` | decimal | Сумма в quote (BNB или `quoteToken`). По документации Flap: у покупки — потрачено, у продажи — получено |
| `fee` | decimal | Комиссия в quote. В данных ≈ 1% от `eth` |
| `postPrice` | decimal | Цена в quote за 1 токен **после** сделки |

`buyer` и `seller` — это `msg.sender` Portal'а. Часто это контракт роутера или бота (например `0x…0f66`), а не кошелёк трейдера. Настоящий кошелёк есть только в `tx.from` транзакции, коллектор его не пишет. Для выборок по адресу: `COALESCE(buyer, seller)`.

---

## Примеры запросов

Сделки Flap в первые 10 секунд жизни токена:

```sql
SELECT t.token, t.symbol, t.quoteToken, t.buyTax, t.sellTax,
       s.type, COALESCE(s.buyer, s.seller) AS trader, s.amount, s.eth, s.postPrice,
       s.block - t.block AS blocks_after_create,
       s.received_at - t.received_at AS ms_after_create
FROM flap_bc_tokens t
JOIN flap_bc_swaps s ON s.token = t.token
WHERE s.received_at - t.received_at BETWEEN 0 AND 10000
ORDER BY t.id, s.block, s.log_index;
```

Первая покупка создателя обычно идёт в той же транзакции, что и создание токена (`s.tx_hash = t.tx_hash`). Её можно отфильтровать, чтобы она не искажала «первый всплеск».

Токены Four.meme в BNB с количеством сделок за первые 5 минут:

```sql
SELECT t.token, t.symbol, COUNT(s.id) AS swaps_5m,
       SUM(s.type = 'TokenPurchase') AS buys, SUM(s.type = 'TokenSale') AS sells
FROM four_meme_bc_tokens t
LEFT JOIN four_meme_bc_swaps s
       ON s.token = t.token AND s.received_at - t.received_at BETWEEN 0 AND 300000
WHERE t.quote = '0x0000000000000000000000000000000000000000'
GROUP BY t.token, t.symbol
ORDER BY swaps_5m DESC;
```

Сделки по токенам, созданным до запуска коллектора, тоже пишутся, но строки в таблице токенов для них нет. `JOIN` с таблицей токенов отсекает их автоматически.
