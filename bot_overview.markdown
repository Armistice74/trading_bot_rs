Trading Bot Overview
This document provides a high-level overview of the trading bot codebase, detailing the purpose and description of each file, along with a comprehensive list of functions, structs, enums, and key components defined in each file. This serves as a quick reference without sharing the full source code. Specific files can be requested for further details.

### Trade Loop Functionality
The bot enters independent per-pair trade loops after WS connections and subscriptions are confirmed via ready channels (with 30s timeout per WS, proceeding on timeout with warning). Each pair's loop runs concurrently, awaiting an OrderComplete signal (e.g., Success from monitor, DelayElapsed from timer, or Shutdown). On signal, if loop_delay_seconds has elapsed since last run and no open orders for the pair, it fetches market data, executes buy logic, then sell logic. The bot relies on WebSocket for real-time data as the primary source, with REST API fallbacks in cases of stale data. Fetching via `fetch_and_store_market_data` uses recent WebSocket data (timestamp <= `data_fetch_period`); falls back to REST if stale. **Note: Kraken's OHLC API delivers timestamps as JSON numbers (f64 Unix seconds with fractional precision), not strings; code converts via .as_f64().to_string() before parsing to handle this accurately and avoid silent drops.**

#### Buy Logic
For each pair independently, the bot checks market conditions (e.g., SMA crossover, ATR volatility spike, ROC momentum) if `conditional_buying` is true. If conditions pass, it computes trade_usd from `trade_percentage` of USD balance (adjusted for liquidity), ensures trade_usd > `min_notional`, then checks if the crypto has experienced a dip meeting `dip_percentage` below the highest price in `period_minutes`. If yes, it places a buy order via actor. Logic skips if price data unavailable or other checks fail.

#### Sell Logic
For each pair independently, the bot fetches open trades (open=1 OR partial_open=1, ordered by timestamp). For each open=1 trade, it evaluates sell conditions: profit_target (or decayed to take_profit_min over profit_decay_period), or stop loss (fixed/trailing). Aggregates prior partial_open=1 trades for weighted average buy price and total quantity. Includes race check (skips if recent sell within 60s for any buy_trade_id). If triggered, executes sell via actor, passing buy_trade_ids. No balance checks; relies on trades table. Includes logging for skips (e.g., no trigger) and debugging (decision close_price vs. thresholds).

##### Post-Sell Trade Closing Logic
A sell trade is either the sell of one trade in the trades table with open=1 or an aggregate of one trade with open=1 and a number of trades of the same pair with partial_open=1 as determined by the sell logic. When a sell trade is filled either partially or fully then the remaining_amount in those corresponding trade(s) (a trade with open=1 or the aggregate of a trade with open=1 and one or more trades with partial_open=1) is reduced by the amount that sell trade was filled. In the case of an aggregate sell, the corresponding trades are reduced in order of trades with partial_open=1 first. If the remaining_amount of trades with partial_open=1 is reduced to 0 then partial_open is set to 0 (this is now considered a closed trade), and then trades with open=1 are reduced by whatever amount from the sell trade is left, if the remaining_amount of the open=1 trade is reduced to 0 then open is set to 0 (this is now considered a closed trade). In the case of a non-aggregate sell then the open=1 trade is reduced by the full sell amount. In either scenario, if, after reducing the remaining_amount by the amount filled in a corresponding sell trade, the remaining_amount is > min_notional then open=1, and if the remaining_amount < min_notional then open is set to 0 and partial_open is set to 1. In this manner an open=1 trade can become a partial_open=1 trade if sold via a partial sell or a partial aggregate sell. Open=1 and partial_open=1 are mutually exclusive such that a trade will only ever be one or the other. Open=1 marks trades where the remaining_amount > min_notional and partial_open=1 marks trades where remaining_amount < min_notional. A partially or fully filled sell trade will always have a remaining_amount of 0 and thus will always have open=0 and partial_open=0. These 3 variables are for buy trades only.

##### Partial Sell Profit Calculation
For partial fills on sell orders (e.g., 0.5 out of 1.0 qty), the profit/loss (PL) for that partial sell trade uses the weighted average_buy_price scaled to the partial qty, without accounting for buy fees (which are tracked separately as negative profit in buy trades). Fees are only the sell-side fees. This ensures per-trade PL reflects sell proceeds minus sell fees and scaled buy cost, with overall accuracy maintained via separate aggregation of buy fees and position-level PL.

<File: api.rs
Purpose: Handles interactions with the Kraken REST API, including authentication, order creation, and market data retrieval.
Description: Encapsulates REST API calls to Kraken, including request signing, retry logic for errors (e.g., nonce issues, rate limits), and specific endpoints for balances, orders, tickers, OHLC, and order books. Includes WS token fetching/refresh for private subs (used by websocket.rs). Public getters added for fields like base_url, http_client, api_key, ws_token, and startup_time to support decoupled access (e.g., in fetch.rs). Added semaphore-based rate limiter (5 concurrent), default max_retries=3, enhanced backoff on "EAPI:Rate limit exceeded" (10s fixed + 5*2^retries exp). Numerical params (e.g., amount, limit_price in create_order) use Decimal for precision; responses as Value for later parsing.
Key Components
Structs
KrakenClient: Manages API key, secret, base URL, HTTP client, startup time, WS token, semaphore (Arc<semaphore>).
WsToken: Holds token and expiry for private WS subs.
Functions/Methods
KrakenClient::new(api_key: String, api_secret: String) -> Self: Initializes the client with validation and semaphore.
KrakenClient::init_ws_token(&mut self) -> Result<()>: Fetches initial WS token.
KrakenClient::fetch_ws_token(&self) -> Result<wstoken>: Signed POST to /private/GetWebSocketsToken.
KrakenClient::refresh_ws_token(&mut self) -> Result<()>: Refreshes if <1h expiry, with backoff retries.
KrakenClient::sign_request(path: &str, nonce: u64, data: &str) -> Result<string>: Signs API requests using HMAC-SHA512.
KrakenClient::api_call_with_retry<T, F, Fut>(func: F, max_retries: u32, log_prefix: &str) -> Result<t>: Retries API calls with backoff and semaphore acquire (enhanced rate limit handling: 10s + 5<<retries s).
KrakenClient::fetch_time() -> Result<u64>: Fetches server time in milliseconds.
KrakenClient::fetch_balance() -> Result<value>: Fetches account balances.
KrakenClient::create_order(pair: &str, order_type: &str, amount: Decimal, limit_price: Decimal, post_only: bool) -> Result<string>: Creates limit orders (max_retries=7).
KrakenClient::fetch_order(_pair: &str, order_id: &str) -> Result<value>: Fetches order details (max_retries=7).
KrakenClient::cancel_order(pair: &str, order_id: &str) -> Result<()>: Cancels orders (max_retries=7).
KrakenClient::fetch_ticker(pair: &str) -> Result<value>: Fetches ticker data (max_retries=7).
KrakenClient::fetch_market_data(pair: &str, interval: i32) -> Result<value>: Fetches OHLC data (max_retries=7).
KrakenClient::fetch_order_book(pair: &str, count: i32) -> Result<value>: Fetches order book (max_retries=7).
KrakenClient::base_url(&self) -> &str: Getter for base URL.
KrakenClient::http_client(&self) -> &Client: Getter for HTTP client.
KrakenClient::api_key(&self) -> &str: Getter for API key.
KrakenClient::ws_token(&self) -> &Option<wstoken>: Getter for WS token.
KrakenClient::startup_time(&self) -> u64: Getter for startup time.

<File: websocket.rs
Purpose: Manages WebSocket connections to Kraken for real-time OHLC, price updates, and private executions/open_orders using v2.
Description: Establishes and maintains separate public (OHLC and ticker) and private (executions combining ownTrades + openOrders) WebSocket v2 connections, subscribes to channels, processes incoming messages, sends updates via channels, and handles token refresh/resub on expiry. Includes retry logic for connections and subscriptions, with polling for sub confirmation (5x 10s, log failures to error_log.txt). Time filter normalized (startup ms to seconds) with logging on skips. Uses getters from api.rs for token/startup access. Executions processing fetches exact reasons via TradeManager (open/cache) before Trade creation, with fallback logging. Order status updates parse updates (order_id, status, vol_exec, pair from symbol), sends UpdateOrderStatus to TradeManager; handles initial snapshot upsert and error resub. OHLC updates now parse precise candle timestamps from "interval_begin" (RFC3339 string) via chrono::DateTime::parse_from_rfc3339 for accuracy in staleness checks. Ticker updates provide real-time last trade (close_price), bid, and ask prices. Public WS spawned in main.rs with retry wrapper; feeds process_ohlc_updates/process_price_updates for trade loop. Added ready channels (mpsc::Sender<()>) to signal initial sub confirmation, sent only once. Public WS subscribes to ohlc channel with interval=1, snapshot=true, and ticker with snapshot=true. Private subscribes to executions with snapshot=true. Includes ping/pong every 20s, heartbeat handling, inactivity timeout (60s) for reconnects. Added auto-resub on subscription errors (up to 3 retries with 5s delay). Numerical fields (e.g., open, high, low, close, volume in OHLC; close, bid, ask in ticker; amount, price, fees, vol_exec in executions) use rust_decimal::Decimal for precision, parsed via Decimal::from_str_exact/from_scientific on JSON strings to avoid floating-point errors.
Key Components
Functions
watch_kraken_prices(pairs: Vec<String>, tx: mpsc::Sender<PriceUpdate>, ohlc_tx: mpsc::Sender<OhlcUpdate>, state_manager: Arc<StateManager>, client: KrakenClient, ready_tx: mpsc::Sender<()>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>: Public WS v2: Connects to wss://ws.kraken.com/v2, subscribes to ohlc (interval=1) and ticker, processes updates with precise timestamps and Decimal parsing for OHLC/ticker values, sends ready on initial confirm, auto-resubs on errors.
watch_kraken_private(state_manager: Arc<StateManager>, client: KrakenClient, ready_tx: mpsc::Sender<()>) -> Result<(), Box<dyn std::error::Error + Send + Sync>>: Private WS v2: Connects to wss://ws-auth.kraken.com/v2, subscribes to executions (with token, confirmation polling), processes trades (filters time, GetOrderReason for reason, Decimal parsing for amount/price/fees, update_trades) and order statuses (Decimal parsing for vol_exec, UpdateOrderStatus for order_id/status/vol_exec/pair), handles token expiry resub, sends ready on initial confirm.

<File: fetch.rs
Purpose: Neutral layer for trade fetching, decoupling WS/API sources.
Description: Centralizes trade ingestion: primary WS (placeholder, assumes pushes), fallback API via fetch_trades_via_api (server-side filtering since startup_time with start/end ms params). Filters trades >= startup_time && >= now-24h, formats IDs (hyphenated), extracts per-trade pair from obj["pair"], dedups via DB check, upserts new to state_manager. Uses api.rs getters for requests. fetch_trades_via_api now logs skips for trades >24h pre-bot to confirm no unintended ingest. Updated to fixed 10s retries (via config.delays.monitor_partial_delay), cap max_retries=3, with optional override for minimal retries (e.g., at timeout). Retries only on empties/errors; immediate return on trades found. Numerical fields (amount, price, fees) parsed via Decimal::from_str_exact for precision; retry_delay_secs as Decimal, converted to f64 for sleep durations.
Key Components
Functions
fetch_trades(client: &KrakenClient, pair: &str, order_id: &str, startup_time: u64, state_manager: Arc<StateManager>, config: &Config, max_retries_override: Option<usize>) -> Result<Vec<Trade>>: Primary fetch (WS no-op if enabled, else API); returns newly inserted Trades. Passes config and override to via_api.
fetch_trades_via_api(client: &KrakenClient, pair: &str, order_id: &str, startup_time: u64, config: &Config, max_retries_override: Option<usize>) -> Result<Vec<(Value, String)>>: Internal API fetch with QueryOrders/TradesHistory fallback (pair extraction, strict time filter, log >24h skips). Fixed retry delay, capped retries with override.

<File: db.rs
Purpose: Manages PostgreSQL database connections, table initialization, and CRUD operations for positions, trades, and market data.
Description: Creates connection pools with retries, initializes tables for positions, trades (with timestamp index), per-pair market data (with timestamp index). Uses NUMERIC(38,18) for numerical fields to ensure exact decimal storage and retrieval, aligned with rust_decimal for precision. Handles updates, inserts, and queries with retry logic for failures (max 3, exp backoff), parsing numerical fields to Decimal. Includes operations for stop-loss timestamps and total PL; update_trade checks pair mismatch (logs to error_log.txt, skips pair update on conflict). get_open_trades queries open=1 OR partial_open=1 (ordered by timestamp for aggregation). get_trades_for_order filters timestamp >= startup (TEXT comparison). insert_trade skips on duplicate. get_total_pl_for_pair sums profit; get_total_pl_for_crypto sums total_pl excl. USD.
Key Components
Functions
create_pool(config: &Config) -> Result<Pool>: Creates Deadpool connection pool with retries.
init_db(config: &Config) -> Result<()>: Initializes database tables and indexes with NUMERIC(38,18) for precision.
update_position(client: &mut Client, pair: &str, position: &Position, is_buy: bool) -> Result<()>: Upserts positions with retries (Decimal params), handles USD consistently with INSERT ON CONFLICT.
update_trade(client: &mut Client, trade_data: &Trade) -> Result<()>: Upserts trades with retries (pair mismatch handling/logging to error_log.txt, skips pair update).
store_market_data(client: &mut Client, market_data: &MarketData) -> Result<()>: Inserts market data ON CONFLICT DO NOTHING with retries (Decimal fields).
get_all_positions(client: &Client) -> Result<Vec<Position>>: Fetches all positions with retries (Decimal parsing).
get_open_trades(client: &Client, pair: &str) -> Result<Vec<Trade>>: Fetches open trades (open=1 OR partial_open=1, ordered by timestamp; Decimal parsing) with retries.
get_market_data_history(client: &Client, pair: &str, since: &str, limit: i64) -> Result<Vec<MarketDataHistory>>: Fetches market history (high/low/close) with retries (Decimal parsing).
get_trade_fees(client: &Client, pair: &str, trade_id: &str) -> Result<Option<Decimal>>: Fetches trade fees as Decimal with retries.
check_trade_exists(client: &Client, pair: &str, trade_id: &str) -> Result<bool>: Checks if trade exists with retries.
insert_trade(client: &mut Client, trade_data: &Trade) -> Result<()>: Inserts trades with retries, skipping duplicates (Decimal params).
get_trades_for_order(client: &Client, order_id: &str, pair: &str, startup_time: u64) -> Result<Vec<Trade>>: Fetches trades for order_id and pair with timestamp >= startup (TEXT; Decimal parsing) with retries.
get_latest_stop_loss_timestamp(client: &Client, pair: &str) -> Result<Option<String>>: Fetches latest sell timestamp for fixed/trailing_stop with retries.
get_total_pl_for_pair(client: &Client, pair: &str) -> Result<Decimal>: Sums profit for pair with retries.
get_total_pl_for_crypto(client: &Client) -> Result<Decimal>: Sums total_pl excl. USD with retries.

<File: actors.rs
Purpose: Defines actor functions for concurrent state management of positions, prices, market data, trades/orders, buy/sell execution, and database ops.
Description: Implements message-passing actors for thread-safe updates/queries. Actors run in async tasks, handling specific domains with channels for communication. Database actor includes caching and retry for failures (backoff on UpdateTrades, periodic retry), flushes cache if >1000 on UpdateTrades (batches 100). trade_manager_actor manages open_orders/trade_ids, caches closed order reasons (TTL 1h, size 1000) and order_statuses for WS openOrders updates (UpdateOrderStatus: upsert status/vol_exec/pair, cache reason/remove on closed/canceled; GetOrderStatus/GetOrderReason queries), db_cache for cancellations, monitoring flags. positions_actor manages positions/balance_cache (invalidate on qty change, GetCachedBalance: refetch if stale with fallback to stale on error). Precision: Uses Decimal for numerical fields; logging with round_dp. Shutdown flushes caches, logs failed entries.
Key Components
Functions
positions_actor(client: Arc<KrakenClient>, mut rx: mpsc::Receiver<PositionsMessage>): Manages positions HashMap with Decimal balance_cache (refetch on stale/uncached via fetch_balance_for_pair, fallback to stale on error).
fetch_balance_for_pair(client: &Arc<KrakenClient>, pair: &str) -> Result<Decimal>: Loads config for key, fetches/parses balance as Decimal.
price_actor(pair: String, mut rx: mpsc::Receiver<PriceMessage>): Manages PriceData get/update.
market_data_actor(pair: String, mut rx: mpsc::Receiver<MarketDataMessage>): Manages MarketData (UpdateOhlc assigns Decimals).
trade_manager_actor(mut rx: mpsc::Receiver<TradeManagerMessage>): Manages open_orders (HashMap<pair, HashMap<order_id, OrderInfo>>), trade_ids (HashMap<pair, HashSet<trade_id>>), closed_reasons (trim TTL/size), order_statuses (HashMap<order_id, OrderStatus>); handles updates/removes (cache reason on close), gets, add/remove/has trade_id, GetOrderReason (open/cached), UpdateOrderStatus (upsert/remove on closed), GetOrderStatus, CacheCancellation, Is/SetMonitoring.
execute_buy_actor(client: Arc<KrakenClient>, state_manager: Arc<StateManager>, mut rx: mpsc::Receiver<BuyMessage>): Executes "buy" create_order, stores OrderInfo (amount/start/timeout/reason/type/symbol).
execute_sell_actor(client: Arc<KrakenClient>, state_manager: Arc<StateManager>, mut rx: mpsc::Receiver<SellMessage>): Executes "sell" create_order, stores OrderInfo with buy_trade_ids/total_buy_qty.
database_actor(config: Config, mut rx: mpsc::Receiver<DatabaseMessage>): Handles DB ops with pool/caching (UpdateTrades retries/backoff, periodic retry, flush >1000); variants: InitializeTables (positions/trades/per-pair market_data), UpdatePositions/StoreMarketData (cache on fail), UpdateTrades/InsertTrade (cache on fail), Gets (TradesForOrder/AllPositions/OpenTrades/MarketDataHistory/TradeFees/CheckTradeExists/LatestStopLossTimestamp/TotalPlForPair/TotalPlForCrypto), Shutdown (flushes cache incl. trade_manager, logs failed to error_log.txt).

<File: statemanager.rs
Purpose: Coordinates state management using actors for positions, prices, market data, trades/orders, buy/sell execution, and database operations.
Description: Implements message-passing actors for thread-safe updates/queries. update_trades includes post-process hook for full field population (e.g., slippage, profit=-(fees) for buys) via processing.rs if !exists, with re-upsert to DB; queries GetOrderReason if reason unset. TradeManagerMessage extended with GetOrderReason/GetOrderStatus/UpdateOrderStatus for reason/status lookup (WS integration). Added get_cached_balance method for TTL-cached balance fetch via PositionsMessage::GetCachedBalance; StateManager::new Arcs client for positions_actor. Supports historical data via looped store_market_data calls. Numerical fields use Decimal for precision. Added OrderComplete enum for trade loop signals; completion_txs per-pair for awaiting in main loops. Added usd_balance Arc<Mutex<Decimal>> for locked access; methods like update_usd_balance_delta, get_usd_balance_locked. update_positions updates total_pl from get_total_pl_for_pair/get_total_pl_for_crypto.
Key Components
Structs
StateManager: Central coordinator with tx channels to actors, running flag, completion_txs, usd_balance Arc<Mutex<Decimal>>.
Position, PriceData, OrderInfo (with average_buy_price), MarketData, Trade, MarketDataHistory, OhlcUpdate, PriceUpdate, OrderStatus: Core data structs (OrderStatus: order_id, pair, status, vol_exec, vol; numerical fields as Decimal).
Enums
PositionsMessage (added GetCachedBalance { pair, ttl, reply }), PriceMessage, MarketDataMessage, TradeManagerMessage (added GetOrderReason/GetOrderStatus/UpdateOrderStatus), BuyMessage, SellMessage, DatabaseMessage, OrderComplete: Actor messages.
Functions/Methods
StateManager::new(config: &Config, client: KrakenClient) -> Result<Arc<self>>: Spawns actors and channels (Arcs client for positions_actor).
get_cached_balance(&self, pair: String, ttl: Duration) -> Result<Option<Decimal>>: Queries cached base asset balance (refetch if stale).
update_trades(&self, trade_data: Trade) -> Result<()>: If reason unset, queries GetOrderReason; if !exists, post-processes/re-upserts for complete fields.
get_position, update_positions, get_price, update_price, store_market_data, get_open_trades, etc. (as before).
send_completion(&self, pair: String, result: OrderComplete) -> Result<()>: Sends signal to per-pair completion channel.
get_usd_balance_locked(&self) -> Result<Decimal>: Returns locked USD balance.
update_usd_balance_delta(&self, delta: Decimal) -> Result<()>: Updates locked USD balance and position.

<File: trading.rs
Purpose: Core trading logic for buy/sell decisions, market data fetching, quantity adjustments, order execution via actors, and global sweeping for lingering orders.
Description: Defines the trading_logic module containing functions for per-pair buy/sell evaluations, including market condition checks, dip detection, profit decay, stop-loss triggers, and post-sell updates to buy trades (following Post-Sell Trade Closing Logic). Uses evalexpr for dynamic limit price calculations based on config formulas. Prefers recent WebSocket data for market fetches with REST fallbacks for OHLC and order books; prioritizes ticker for close_price/bid/ask if fresher. Executes orders asynchronously via actor channels, spawns monitoring tasks, and uses completion receivers to await signals. Includes race prevention in sells (60s TTL on recent sells), stop-loss pause for buys, and global sweep every 60s for lingering orders >30s. Numerical operations use Decimal for precision; handles shutdown signals. Added logging for sell skips (mirroring buy_logic) and debugging (decision close_price vs. thresholds/fills).
Key Components
Functions (in trading_logic mod)
get_balance_key_for_pair(pair: &str, config: &Config) -> String: Determines balance key for pair from config or extracts from symbol (warns if extracted).
fetch_and_store_market_data(pair: &str, client: &KrakenClient, config: &Config, state_manager: Arc<StateManager>) -> Result<()>: Fetches/stores OHLC (WS-preferred if <= data_fetch_period, else REST) and order book (always REST), merges data, updates prices; skips on incomplete data; prioritizes ticker for close_price/bid/ask.
adjust_quantity(pair: &str, qty: Decimal, price: Decimal, symbol_info: &SymbolInfo, min_notional_config: Decimal) -> Option<Decimal>: Adjusts qty for lot size, precision, min/max, and min_notional checks; returns None if invalid.
buy_logic(pair: &str, state_manager: Arc<StateManager>, client: &KrakenClient, config: &Config, completion_rx: &mut UnboundedReceiver<OrderComplete>, shutdown_tx: broadcast::Sender<()>) -> Result<(bool, Decimal)>: Checks stop-loss pause, market conditions, dip threshold; computes trade_usd, adjusts amount; executes buy if conditions met, awaits completion signal; returns (triggered, updated_usd).
sell_logic(pair: &str, state_manager: Arc<StateManager>, client: &KrakenClient, config: &Config, completion_rx: &mut UnboundedReceiver<OrderComplete>, shutdown_tx: broadcast::Sender<()>) -> Result<bool>: Fetches/sorts open trades, evaluates each for sell (with race skip on recent sells), executes if triggered, awaits all completions; maintains recent_sells HashMap (60s TTL); logs skips/no-triggers.
evaluate_trade(trade: &Trade, pair: &str, close_price: Decimal, config: &Config, state_manager: Arc<StateManager>, client: &KrakenClient, symbol_info: &SymbolInfo, open_trades: &Vec<Trade>) -> Result<Option<(String, Vec<String>, Decimal, String, Decimal, Decimal)>>: Evaluates profit (with decay) or stop-loss; aggregates partial trades for weighted avg buy price/total qty; adjusts amount; returns sell params if triggered; logs skips and decision details.
execute_buy_trade(client: &KrakenClient, pair: &str, amount: Decimal, reason: &str, average_buy_price: Decimal, symbol_info: &SymbolInfo, close_price: Decimal, config: &Config, state_manager: Arc<StateManager>, shutdown_tx: broadcast::Sender<()>) -> Result<(Option<String>, Decimal, Decimal, Decimal, String, Decimal, Option<String>, Decimal, Decimal, Decimal)>: Evaluates limit price via config formula (fallback to close - slippage), checks USD balance/fee; sends to buy actor, spawns monitor task if not already monitoring.
execute_sell_trade(client: KrakenClient, pair: String, amount: Decimal, reason: String, average_buy_price: Decimal, symbol_info: SymbolInfo, close_price: Decimal, config: Config, state_manager: Arc<StateManager>, buy_trade_id: String, buy_trade_ids: Vec<String>, total_buy_qty: Decimal, shutdown_tx: broadcast::Sender<()>) -> Result<Option<(String, Decimal, Decimal, Decimal, String, Decimal, Option<String>, Decimal, Decimal, Vec<String>, Decimal)>>: Similar to buy but for sell; sends to sell actor with buy_trade_ids/total_buy_qty, spawns monitor; logs decision close_price.
get_highest_price_period(client: &KrakenClient, pair: &str, period_minutes: i32, current_price: Decimal, state_manager: Arc<StateManager>) -> Decimal: Fetches history high over period (DB or REST ticker fallback); returns max high or current_price on error.
update_remaining_quantities(pair: &str, net_qty: Decimal, trade_ids_to_sell: Vec<String>, state_manager: Arc<StateManager>, config: &Config, base_currency: &str) -> Result<()>: Reduces remaining_amount in buy trades sequentially (partials first), updates open/partial_open flags based on min_notional/close_price; removes tiny trades (<=1e-8).
global_trade_sweep(state_manager: Arc<StateManager>, client: &KrakenClient, config: &Config, shutdown_tx: broadcast::Sender<()>) -> Result<()>: Loops every 60s (WS always enabled), scans open orders per pair; spawns monitors for lingering (>30s) orders if not already monitoring; handles shutdown.

<File: monitor.rs
Purpose: Handles order monitoring logic, including polling for status, trade fetches, and processing with dedup avoidance.
Description: Monitors specific orders for fills, supports pre-fetched trades to avoid dups, handles partial fills with accumulation loop (partial_delay sleep until vol_exec >= vol or closed/timeout, stagnant threshold=3 to escalate cancel), rate limit backoff, slippage checks (best_bid/ask), cancellation on timeout/conditions/slippage. Uses fetch_trades for new trades (skips if WS no fills and not near 80% timeout; minimal retries at timeout), process_trade_data for updates (passing pre-fetched), and update_remaining_quantities for sells (targeted via buy_trade_ids param, fallback from OrderInfo/DB if empty). Queries GetOrderStatus before API fetch_order (skip on closed/vol_exec>0, use status.vol for partials); 30s loop fallback if no WS trigger; API fallback logged at ERROR level with reason. Note: Sell monitoring should ensure post-fill updates to buy trades follow the Post-Sell Buy Trade Closing Logic in Sell Logic section, including targeted reductions and flag updates via update_remaining_quantities. Updated to skip fetch_trades if WS confirms no fills and not near timeout (80% threshold); at timeout, final fetch with minimal retries (override=1). Adds 1s sleep post-signal before first check. Singleton check/set monitoring flag to avoid duplicates. Added debugging logs for sell fills (actual avg_price/fees vs. decision close_price).
Key Components
Functions
monitor_order(client: &KrakenClient, pairs: Vec<String>, state_manager: Arc<StateManager>, config: &Config, target_order_id: String, average_buy_price: Decimal, buy_trade_ids: Vec<String>, total_buy_qty: Decimal, shutdown_rx: BroadcastReceiver<()>) -> Result<(bool, Decimal, Decimal, Decimal, Vec<String>)>: Monitors order, returns filled, qty, fees, avg_price, trade_ids (sells: targeted sequential decrements; WS query skip API on closed/partial). Handles shutdown, accumulation/escalate on stagnant, processes trades/updates quantities/USD/remove open/send completion; logs fill details for sells.
clear_monitoring_flag(state_manager: &Arc<StateManager>, order_id: &str): Clears monitoring flag.
get_order_status(state_manager: &Arc<StateManager>, order_id: &str) -> Option<OrderStatus>: Queries GetOrderStatus.
is_terminal(status: &str) -> bool: Checks if status is closed/canceled/filled/expired.
escalate_to_cancel(client: &KrakenClient, symbol: &str, order_id: &str, pair_item: &str, state_manager: Arc<StateManager>): Cancels order, removes open.

<File: main.rs
Purpose: Entry point for bot startup, configuration loading, component initialization, and task orchestration.
Description: Sets up logging (cmd_log.txt, error_log.txt, trades_log.txt with target="trade"), loads/validates config, validates API creds (trim/base64), initializes KrakenClient (init_ws_token), validates auth (fetch_balance, USD parse), inits DB, inits StateManager (positions incl. USD with retries, sync_usd_balance with retries). Spawns public WS (prices/OHLC), private WS (executions), price/OHLC processors, global_trade_sweep. Awaits WS ready with 30s timeout per (proceed with warning). Spawns per-pair trading loops (concurrent, each awaits shutdown/sleep, no opens: fetch market, buy_logic (await completion), sell_logic (await)). Handles shutdown via ctrl-c/task term (broadcast channel). Numerical handling updated to Decimal for precision (e.g., usd_balance, durations via to_f64()).
Key Components
Functions
process_price_updates(state_manager: Arc<StateManager>, mut rx: mpsc::Receiver<PriceUpdate>): Processes price updates to state.
process_ohlc_updates(state_manager: Arc<StateManager>, mut rx: mpsc::Receiver<OhlcUpdate>): Processes OHLC updates via market_data_txs.
get_current_time() -> DateTime<Local>: Returns local time.
parse_est_timestamp(timestamp: &str) -> Option<DateTime<Local>>: Parses timestamp (multiple formats).
map_ws_pair(api_pair: &str, config: &Config) -> String: Maps API to WS pairs.
init_metrics(): Initializes metrics (tracing stub).
record_api_latency(endpoint: &str, duration: f64): Logs latency.
record_trade_count(pair: &str, trade_type: &str, trade: &Trade): Logs trades (target="trade").
record_error(error_type: &str): Logs errors.
validate_api_credentials(config: &Config) -> Result<(String, String)>: Validates/ cleans keys (trim/base64).
initialize_kraken_client(api_key: String, api_secret: String) -> Result<KrakenClient>: Initializes client, tests fetch_time.
validate_kraken_authentication(kraken_client: &KrakenClient) -> Result<Decimal>: Validates auth, gets USD balance (ZUSD parse).
initialize_database(config: &Config) -> Result<()>: Initializes DB.
initialize_state_manager(config: &Config, kraken_client: KrakenClient, usd_balance: Decimal) -> Result<Arc<StateManager>>: Initializes state, upserts positions (retries), syncs USD (retries).
setup_websocket_channels(config: &Config) -> Result<(Sender<PriceUpdate>, Receiver<PriceUpdate>, Sender<OhlcUpdate>, Receiver<OhlcUpdate>)>: Sets up channels.
main() -> Result<()>: Orchestrates startup/WS spawn (public/private always enabled) with state/client params, awaits ready with timeout, spawns processors/sweep/trading loops; handles shutdown.

<File: config.rs
Purpose: Defines structs for deserializing config.toml and validation logic.
Description: Creates main Config struct and sub-structs for various sections, with default constructors and validation to ensure config integrity. Numerical fields use Decimal for precision; validation updated for Decimal comparisons (e.g., > Decimal::ZERO).
Key Components
Structs
Config, ValueWrapper<t>, ApiKeys, Portfolio, TradingLogic, StopLossBehavior, Database, MarketChecks, BotOperation, PriceLogic, SymbolInfo, LotSize, Delays (numerical fields as Decimal).
Functions/Methods
Config::new() -> Self: Default constructor.
Config::validate(&self) -> Result<(), String>: Validates all config values.
load_config() -> Result<Config>: Loads and parses config.toml, validates.

<File: config.toml
Purpose: TOML configuration file for bot parameters.
Description: Defines sections like [api_keys], [portfolio], [trading_logic], with values and descriptions. database.cache_size increased to 100000 for high-volume handling. delays.monitor_loop_delay=30.0, monitor_partial_delay=10.0 for WS tuning.
Key Components
TOML key-value pairs.

<File: stop.rs
Purpose: Handles per-trade fixed and trailing stop-loss evaluations.
Description: Evaluates stop-loss conditions for trades, determining whether a fixed or trailing stop-loss should trigger a sell. Numerical fields use Decimal for precision in calculations (e.g., percentages, thresholds).
Key Components
Enums
StopType: Fixed, Trailing.
Functions/Methods
check_stop_loss(trade_timestamp: &str, average_buy_price: Decimal, close_price: Decimal, highest_price_since_buy: Decimal, config: &Config) -> Option<StopType>: Evaluates stop triggers (Decimal params).

<File: utils.rs
Purpose: Shared utilities for timestamp parsing and current time.
Description: Defines functions for getting current time and parsing timestamps. Updated parse_unix_timestamp to use Decimal for precision in handling fractional seconds.
Key Components
Functions
get_current_time() -> DateTime<Local>: Returns current local time.
parse_unix_timestamp(unix_str: &str) -> Option<DateTime<Utc>>: Parses Unix timestamp using Decimal for secs/nanos extraction.
get_eastern_tz() -> Local: Returns local timezone.

<File: indicators.rs
Purpose: Contains technical indicators and market condition checks used in trading decisions.
Description: This module includes functions for calculating Simple Moving Average (SMA), Average True Range (ATR), and Rate of Change (ROC), as well as evaluating market conditions for conditional buying based on configurable checks like trend, momentum, and volatility. These are pure computational functions, reusable independently.
Key Components
Functions (in indicators mod)
calculate_sma(pair: &str, sma_period: i32, state_manager: Arc<StateManager>) -> Option<Decimal>: Calculates SMA over history.
calculate_atr(pair: &str, atr_period: i32, state_manager: Arc<StateManager>) -> Decimal: Calculates ATR with fallback.
calculate_roc(pair: &str, roc_period: i32, current_price: Decimal, state_manager: Arc<StateManager>) -> Option<Decimal>: Calculates ROC.
market_condition_check(pair: &str, close_price: Decimal, config: &Config, state_manager: Arc<StateManager>) -> (bool, Vec<String>): Evaluates uptrend, momentum, volatility spike using SMA, ROC, ATR.

<File: processing.rs
Purpose: Handles post-trade processing and updates.
Description: This module manages aggregating trade data from fills, updating positions and USD balances, calculating profits, fees, slippage, and prorated costs for sells, as well as profit percentages for buys based on fees. It also records events and ensures data consistency. Centers on post-trade updates and position management.
Key Components
Functions (in processing mod)
process_trade_data(client: &KrakenClient, pair: &str, trade_type: &str, order_id: &str, _start_time: f64, state_manager: Arc<StateManager>, reason: &str, config: &Config, average_buy_price: Decimal, buy_trade_id: Option<String>, pre_fetched_trades: Option<Vec<Trade>>, total_buy_qty: Option<Decimal>) -> Result<(Decimal, Decimal, Decimal, Vec<String>, Vec<Trade>>: Processes fills, updates positions/USD, computes profit/slippage (supports pre-fetched trades; sells: sequential reductions via targeted IDs, Decimal rounding).
post_process_trade(trade: &mut Trade, pair: &str, _close_price: Decimal, state_manager: &Arc<StateManager>, config: &Config) -> Result<()>: Computes full fields post-insert (fee%, slippage=0, profit=-(fees) for buys, etc.; sells: aggregate total_pl/total_fees re-compute).
compute_weighted_basis_from_trades(pair: &str, buy_trade_ids: Option<Vec<String>>, state_manager: Arc<StateManager>) -> Decimal: Computes weighted average buy price from open trades (filtered by IDs if provided).

### Codebase Architecture Diagram
Below is a visual representation of the trading bot's architecture, showing the flow of data and dependencies between components:
+-------------------+
|  config.toml      |
|  (Configuration)  |
+-------------------+
|
v
+-------------------+
|  config.rs        |
|  (Config Structs  |
|   & Validation)   |
+-------------------+
|
+-------------------+
|                   |
v                   v
+-------------------+  +-------------------+
|  api.rs           |  |  Cargo.toml       |
|  (Kraken REST)    |  |  (Dependencies)   |
+-------------------+  +-------------------+
|                       |
v                       v
+-------------------+  +-------------------+
|  websocket.rs     |  |  db.rs            |
|  (Real-time Data)|  |  (DB Operations)  |
+-------------------+  +-------------------+
|                       |
+-----------+-----------+
|                       |
v                       v
+-------------------+  +-------------------+
|  fetch.rs         |  |  actors.rs        |
|  (Trade Fetch)    |  |  (Actors)         |
+-------------------+  +-------------------+
|                       |
+-----------+-----------+
|
v
+-------------------+
|  statemanager.rs  |
|  (Stat & Actors)  |
+-------------------+
|
v
+-------------------+  +-------------------+  +-------------------+  +-------------------+
|  trading.rs       |  |  monitor.rs       |  |  indicators.rs    |  |  processing.rs    |
|  (Trading Logic)  |  |  (Order Monitor)  |  |  (Indicators)     |  |  (Processing)     |
+-------------------+  +-------------------+  +-------------------+  +-------------------+
|
v
+-------------------+
|  stop.rs          |
|  (Stop Behaviors) |
+-------------------+
|
+-----------+-----------+
|                       |
v                       v
+-------------------+  +-------------------+
|  utils.rs         |  |  main.rs          |
|  (Time Utils)     |  |  (Entry Point)    |
+-------------------+  +-------------------+