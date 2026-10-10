-- Таблицы коллекторов площадок запуска (collectors/). Временное хранилище: collectors/export.js переносит строки
-- в parquet (swaps/<площадка>/…: launchlab, meteora-dbc, pump.fun-amm) и удаляет их из MySQL. Суммы и резервы — сырые целые из событий (DECIMAL), без деления на разряды.
-- ts / created_at_ms — время прихода транзакции из gRPC (мс), slot — слот транзакции.

-- Raydium LaunchLab (LanMV9…): bonk.fun и другие площадки (platform_config)
CREATE TABLE IF NOT EXISTS lp_launchlab_swaps (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  signature CHAR(88) NOT NULL, slot BIGINT NOT NULL, ts BIGINT NOT NULL,
  pool CHAR(44) NOT NULL, mint CHAR(44) NULL, quote_mint CHAR(44) NULL, user CHAR(44) NULL,
  platform_config CHAR(44) NULL, global_config CHAR(44) NULL,
  is_buy TINYINT NOT NULL, exact_in TINYINT NULL, ix VARCHAR(16) NULL,
  amount_in DECIMAL(20,0) NOT NULL, amount_out DECIMAL(20,0) NOT NULL,
  virtual_base DECIMAL(20,0) NOT NULL, virtual_quote DECIMAL(20,0) NOT NULL,
  real_base_before DECIMAL(20,0) NOT NULL, real_quote_before DECIMAL(20,0) NOT NULL,
  real_base_after DECIMAL(20,0) NOT NULL, real_quote_after DECIMAL(20,0) NOT NULL,
  total_base_sell DECIMAL(20,0) NULL,
  protocol_fee DECIMAL(20,0) NULL, platform_fee DECIMAL(20,0) NULL, creator_fee DECIMAL(20,0) NULL, share_fee DECIMAL(20,0) NULL,
  pool_status TINYINT NULL,
  cu_price BIGINT NULL, cu_limit BIGINT NULL, signers_count INT NULL, tips JSON NULL,
  ev_index INT NULL COMMENT 'номер события программы в транзакции',
  KEY lp_launchlab_swaps_pool (pool), KEY lp_launchlab_swaps_ts (ts),
  UNIQUE KEY lp_launchlab_swaps_sig_ev (signature, ev_index)
) ENGINE=InnoDB COMMENT='сделки Raydium LaunchLab (collectors/launchlab.js)';

CREATE TABLE IF NOT EXISTS lp_launchlab_pools (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  pool CHAR(44) NOT NULL, mint CHAR(44) NULL, quote_mint CHAR(44) NULL, creator CHAR(44) NULL, payer CHAR(44) NULL,
  config CHAR(44) NULL, platform_config CHAR(44) NULL,
  decimals TINYINT NULL, name VARCHAR(255) NULL, symbol VARCHAR(255) NULL, uri VARCHAR(512) NULL,
  curve_type VARCHAR(16) NULL, supply DECIMAL(20,0) NULL, total_base_sell DECIMAL(20,0) NULL, total_quote_fund_raising DECIMAL(20,0) NULL,
  migrate_type TINYINT NULL, vesting_locked DECIMAL(20,0) NULL, vesting_cliff DECIMAL(20,0) NULL, vesting_unlock DECIMAL(20,0) NULL,
  amm_fee_on TINYINT NULL,
  created_at_ms BIGINT NOT NULL, slot BIGINT NOT NULL, signature CHAR(88) NOT NULL,
  UNIQUE KEY lp_launchlab_pools_pool (pool)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='пулы (монеты) Raydium LaunchLab';

-- Meteora Dynamic Bonding Curve (dbcij3…): Bags, Believe, Jupiter Studio и др. (config)
CREATE TABLE IF NOT EXISTS lp_meteora_dbc_swaps (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  signature CHAR(88) NOT NULL, slot BIGINT NOT NULL, ts BIGINT NOT NULL,
  pool CHAR(44) NOT NULL, config CHAR(44) NULL, mint CHAR(44) NULL, quote_mint CHAR(44) NULL, user CHAR(44) NULL,
  is_buy TINYINT NOT NULL, has_referral TINYINT NULL, ev TINYINT NOT NULL, swap_mode TINYINT NULL,
  amount_in DECIMAL(20,0) NOT NULL, amount_out DECIMAL(20,0) NOT NULL, param_0 DECIMAL(20,0) NULL, param_1 DECIMAL(20,0) NULL,
  excluded_fee_input DECIMAL(20,0) NULL, amount_left DECIMAL(20,0) NULL,
  next_sqrt_price DECIMAL(39,0) NOT NULL,
  trading_fee DECIMAL(20,0) NULL, protocol_fee DECIMAL(20,0) NULL, referral_fee DECIMAL(20,0) NULL,
  quote_reserve DECIMAL(20,0) NULL, migration_threshold DECIMAL(20,0) NULL, event_ts BIGINT NULL,
  cu_price BIGINT NULL, cu_limit BIGINT NULL, signers_count INT NULL, tips JSON NULL,
  ev_index INT NULL COMMENT 'номер события программы в транзакции',
  KEY lp_meteora_dbc_swaps_pool (pool), KEY lp_meteora_dbc_swaps_ts (ts),
  UNIQUE KEY lp_meteora_dbc_swaps_sig_ev (signature, ev_index)
) ENGINE=InnoDB COMMENT='сделки Meteora DBC (collectors/meteora-dbc.js)';

CREATE TABLE IF NOT EXISTS lp_meteora_dbc_pools (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  pool CHAR(44) NOT NULL, config CHAR(44) NULL, creator CHAR(44) NULL, mint CHAR(44) NULL, quote_mint CHAR(44) NULL, payer CHAR(44) NULL,
  pool_type TINYINT NULL, activation_point DECIMAL(20,0) NULL,
  created_at_ms BIGINT NOT NULL, slot BIGINT NOT NULL, signature CHAR(88) NOT NULL,
  UNIQUE KEY lp_meteora_dbc_pools_pool (pool)
) ENGINE=InnoDB COMMENT='пулы (монеты) Meteora DBC';

CREATE TABLE IF NOT EXISTS lp_meteora_dbc_completes (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  pool CHAR(44) NOT NULL, config CHAR(44) NULL, base_reserve DECIMAL(20,0) NULL, quote_reserve DECIMAL(20,0) NULL,
  ts BIGINT NOT NULL, slot BIGINT NOT NULL, signature CHAR(88) NOT NULL,
  KEY lp_meteora_dbc_completes_pool (pool),
  UNIQUE KEY lp_meteora_dbc_completes_sig_pool (signature, pool)
) ENGINE=InnoDB COMMENT='завершения кривой (миграция) Meteora DBC';

-- pump.fun AMM (pAMMBay…): пулы миграции с кривой pump.fun и сделки в них в первые 60 минут (collectors/amm-run.js);
-- pool_*_reserves — резервы пула до сделки (из события); coin_creator — в lp_pump_amm_pools (один на монету)
CREATE TABLE IF NOT EXISTS lp_pump_amm_swaps (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  signature CHAR(88) NOT NULL, slot BIGINT NOT NULL, ts BIGINT NOT NULL, event_ts BIGINT NULL,
  pool CHAR(44) NOT NULL, mint CHAR(44) NULL, quote_mint CHAR(44) NULL, user CHAR(44) NULL,
  is_buy TINYINT NOT NULL, ix_name VARCHAR(32) NULL,
  base_amount DECIMAL(20,0) NOT NULL, quote_amount DECIMAL(20,0) NOT NULL, user_quote_amount DECIMAL(20,0) NULL, limit_quote_amount DECIMAL(20,0) NULL,
  pool_base_reserves DECIMAL(20,0) NOT NULL, pool_quote_reserves DECIMAL(20,0) NOT NULL,
  lp_fee DECIMAL(20,0) NULL, protocol_fee DECIMAL(20,0) NULL, coin_creator_fee DECIMAL(20,0) NULL, cashback DECIMAL(20,0) NULL, buyback_fee DECIMAL(20,0) NULL,
  lp_fee_bps INT NULL, protocol_fee_bps INT NULL, coin_creator_fee_bps INT NULL,
  cu_price BIGINT NULL, cu_limit BIGINT NULL, signers_count INT NULL, tips JSON NULL,
  src TINYINT NOT NULL DEFAULT 0 COMMENT '0 — поток gRPC (ts — время прихода, мс), 1 — дотянуто из RPC (ts — время блока, с)',
  ev_index INT NULL COMMENT 'номер события программы в транзакции',
  KEY lp_pump_amm_swaps_pool (pool), KEY lp_pump_amm_swaps_ts (ts),
  UNIQUE KEY lp_pump_amm_swaps_sig_ev (signature, ev_index)
) ENGINE=InnoDB COMMENT='сделки pump.fun AMM в первые 60 мин после миграции (collectors/pump-amm.js)';

CREATE TABLE IF NOT EXISTS lp_pump_amm_pools (
  id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
  pool CHAR(44) NOT NULL, mint CHAR(44) NULL, quote_mint CHAR(44) NULL, creator CHAR(44) NULL, coin_creator CHAR(44) NULL,
  pool_index INT NULL, base_decimals TINYINT NULL, quote_decimals TINYINT NULL,
  base_amount_in DECIMAL(20,0) NULL, quote_amount_in DECIMAL(20,0) NULL, lp_mint CHAR(44) NULL, is_mayhem TINYINT NULL,
  created_at_ms BIGINT NOT NULL, slot BIGINT NOT NULL, signature CHAR(88) NOT NULL,
  UNIQUE KEY lp_pump_amm_pools_pool (pool)
) ENGINE=InnoDB COMMENT='пулы pump.fun AMM, созданные миграцией с кривой pump.fun';

-- pump.fun bonding curve (как в copy-trade-solana: пишет sandbox/trader.js при LOG_SWAPS_HISTORY=1, выгружает simulation/export/dataset.parquet.js)
CREATE TABLE IF NOT EXISTS `pump_fun_bc_swaps` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `signature` char(90) DEFAULT NULL,
  `mint` char(44) DEFAULT NULL,
  `user` char(44) DEFAULT NULL,
  `block` int(10) unsigned DEFAULT NULL,
  `amount_sol` decimal(27,9) DEFAULT NULL,
  `vsr` decimal(30,9) DEFAULT NULL,
  `vtr` decimal(18,6) DEFAULT NULL,
  `rsr` decimal(30,9) DEFAULT NULL,
  `rtr` decimal(18,6) DEFAULT NULL,
  `price_sol` decimal(36,18) DEFAULT NULL,
  `amount_token` decimal(15,6) DEFAULT NULL,
  `timestamp` bigint(20) unsigned DEFAULT NULL,
  `cu_price` int(10) unsigned DEFAULT NULL,
  `cu_limit` int(10) unsigned DEFAULT NULL,
  `protocol_fee` decimal(27,9) DEFAULT NULL,
  `creator_fee` decimal(27,9) DEFAULT NULL,
  `signers_count` tinyint(3) unsigned DEFAULT NULL,
  `tips` json DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `pump_fun_bc_swaps_user_index` (`user`),
  KEY `pump_fun_bc_swaps_signature_index` (`signature`),
  KEY `pump_fun_bc_swaps_mint_index` (`mint`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

CREATE TABLE IF NOT EXISTS `pump_fun_bc_mints` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `mint` char(44) DEFAULT NULL,
  `creator` char(44) DEFAULT NULL,
  `created_at_ms` bigint(20) unsigned DEFAULT NULL,
  `mayhem_mode` tinyint(4) DEFAULT '0',
  `quote_mint` varchar(44) DEFAULT 'So11111111111111111111111111111111111111112' COMMENT 'валюта котировки кривой, WSOL — SOL',
  PRIMARY KEY (`id`),
  UNIQUE KEY `pump_fun_bc_mints_pk` (`mint`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ===== BSC (перенесено из node/bsc/schema.sql, collector-amm-01) =====
-- Four.meme bonding curve (TokenManager2 0x5c952063c7fc8610FFDB798152D69F0B9550762b)
-- uint256 amounts are stored divided by 1e18 (exact: DECIMAL keeps all 18 fractional digits)
-- address/hash columns are latin1 everywhere: tokens (utf8mb4 for names) JOIN swaps (latin1) on token must use the index

CREATE TABLE IF NOT EXISTS `four_meme_bc_tokens` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  -- TokenCreate event
  `creator` char(42) CHARACTER SET latin1 NOT NULL,
  `token` char(42) CHARACTER SET latin1 NOT NULL,
  `requestId` decimal(65,0) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `symbol` varchar(255) DEFAULT NULL,
  `totalSupply` decimal(65,18) DEFAULT NULL,
  `launchTime` bigint(20) unsigned DEFAULT NULL,
  `launchFee` decimal(65,18) DEFAULT NULL,
  -- TokenManager2._tokenInfos(token).quote, 0x000...0 = BNB
  `quote` char(42) CHARACTER SET latin1 DEFAULT NULL,
  -- log meta
  `tx_hash` char(66) CHARACTER SET latin1 NOT NULL,
  `log_index` int(10) unsigned NOT NULL,
  `block` int(10) unsigned NOT NULL,
  `block_timestamp` int(10) unsigned DEFAULT NULL,
  `received_at` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `four_meme_bc_tokens_token_unique` (`token`),
  KEY `four_meme_bc_tokens_creator_index` (`creator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `four_meme_bc_swaps` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `type` enum('TokenPurchase','TokenSale') NOT NULL,
  -- TokenPurchase / TokenSale event
  `token` char(42) NOT NULL,
  `account` char(42) NOT NULL,
  `price` decimal(65,18) NOT NULL,
  `amount` decimal(65,18) NOT NULL,
  `cost` decimal(65,18) NOT NULL,
  `fee` decimal(65,18) NOT NULL,
  `offers` decimal(65,18) NOT NULL,
  `funds` decimal(65,18) NOT NULL,
  -- log meta
  `tx_hash` char(66) NOT NULL,
  `log_index` int(10) unsigned NOT NULL,
  `block` int(10) unsigned NOT NULL,
  `block_timestamp` int(10) unsigned DEFAULT NULL,
  `received_at` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `four_meme_bc_swaps_tx_log_unique` (`tx_hash`, `log_index`),
  KEY `four_meme_bc_swaps_token_index` (`token`),
  KEY `four_meme_bc_swaps_account_index` (`account`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- Flap.sh bonding curve (Portal 0xe2cE6ab80874Fa9Fa2aAE65D277Dd6B8e65C9De0)

CREATE TABLE IF NOT EXISTS `flap_bc_tokens` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  -- TokenCreated event
  `ts` bigint(20) unsigned NOT NULL,
  `creator` char(42) CHARACTER SET latin1 NOT NULL,
  `nonce` decimal(65,0) DEFAULT NULL,
  `token` char(42) CHARACTER SET latin1 NOT NULL,
  `name` varchar(255) DEFAULT NULL,
  `symbol` varchar(255) DEFAULT NULL,
  `meta` varchar(1024) DEFAULT NULL,
  -- TokenQuoteSet event, 0x000...0 = BNB
  `quoteToken` char(42) CHARACTER SET latin1 DEFAULT NULL,
  -- FlapTokenTaxSet / FlapTokenAsymmetricTaxSet events, basis points (100 = 1%)
  `tax` int(10) unsigned DEFAULT NULL,
  `buyTax` int(10) unsigned DEFAULT NULL,
  `sellTax` int(10) unsigned DEFAULT NULL,
  -- log meta
  `tx_hash` char(66) CHARACTER SET latin1 NOT NULL,
  `log_index` int(10) unsigned NOT NULL,
  `block` int(10) unsigned NOT NULL,
  `block_timestamp` int(10) unsigned DEFAULT NULL,
  `received_at` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `flap_bc_tokens_token_unique` (`token`),
  KEY `flap_bc_tokens_creator_index` (`creator`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS `flap_bc_swaps` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `type` enum('TokenBought','TokenSold') NOT NULL,
  -- TokenBought / TokenSold event
  `ts` bigint(20) unsigned NOT NULL,
  `token` char(42) NOT NULL,
  `buyer` char(42) DEFAULT NULL,
  `seller` char(42) DEFAULT NULL,
  `amount` decimal(65,18) NOT NULL,
  `eth` decimal(65,18) NOT NULL,
  `fee` decimal(65,18) NOT NULL,
  `postPrice` decimal(65,18) NOT NULL,
  -- log meta
  `tx_hash` char(66) NOT NULL,
  `log_index` int(10) unsigned NOT NULL,
  `block` int(10) unsigned NOT NULL,
  `block_timestamp` int(10) unsigned DEFAULT NULL,
  `received_at` bigint(20) unsigned NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `flap_bc_swaps_tx_log_unique` (`tx_hash`, `log_index`),
  KEY `flap_bc_swaps_token_index` (`token`),
  KEY `flap_bc_swaps_buyer_index` (`buyer`),
  KEY `flap_bc_swaps_seller_index` (`seller`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;

-- ===== Метадата монет pump.fun (src/sources/pump-meta.js; раньше писал трейдер copy-trade-solana с LOG_META=1) =====
CREATE TABLE IF NOT EXISTS `pump_fun_bc_meta` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT,
  `mint` varchar(255) DEFAULT NULL,
  `url` varchar(255) DEFAULT NULL,
  `name` varchar(255) DEFAULT NULL,
  `symbol` varchar(255) DEFAULT NULL,
  `image` json DEFAULT NULL,
  `data` json DEFAULT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=latin1;
