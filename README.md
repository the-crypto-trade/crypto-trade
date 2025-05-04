<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Crypto Trade](#crypto-trade)
  - [Branches](#branches)
  - [Installation](#installation)
    - [Install From Github](#install-from-github)
    - [Install Locally](#install-locally)
  - [API](#api)
  - [Examples](#examples)
  - [Thread Safety](#thread-safety)
  - [Performance Tuning](#performance-tuning)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Crypto Trade
* A pure Python library for trading on cryptocurrency exchanges.
* More than a library, it syncs exchange data to local memory and keeps track of realtime data updates.
* Unified API for different exchanges.
* Super simple to use. Your code will be in a linear and synchronous manner. The library takes care of asynchronous networking.
* Supported exchanges:
  * [Bybit](https://www.bybit.com/invite?ref=XNYP2K).
  * [OKX](https://www.okx.com/join/47636709).
  * More coming soon...

## Branches
* The `main` branch may contain experimental features.
* The `release` branch represents the most recent stable release.

## Installation
* It's recommended that you install in a virtual environment of your choosing.

### Install From Github

    pip install git+https://github.com/the-crypto-trade/crypto-trade

### Install Locally

    pip install -e '.[dev]'

## API
* Initialize.
```
class Exchange(ExchangeApi):

    def __init__(
        self,
        *,
        name: str,
        exchange_id: Optional[str] = None,  # arbitrary user-defined data
        symbols: str | Iterable[str] = "*",  # a comma-separated string or an iterable of strings. Use '*' to represent all symbols that are open for trade.
        instrument_type: Optional[str] = None,  # Defaults to spot type. See each derived exchange class for allowed values for that exchange.
        margin_asset: Optional[str] = None,
        # bbo
        subscribe_bbo: bool = False,
        # trade
        subscribe_trade: bool = False,
        fetch_historical_trade_at_start: bool = False,
        fetch_historical_trade_start_unix_timestamp_seconds: Optional[int] = None,
        fetch_historical_trade_end_unix_timestamp_seconds: Optional[int] = None,
        keep_historical_trade_seconds: Optional[int] = 300,  # max historical data time span
        remove_historical_trade_interval_seconds: Optional[int] = 60,  # how often to remove
        # ohlcv
        subscribe_ohlcv: bool = False,
        ohlcv_interval_seconds: int = 60,
        is_ohlcv_interval_aligned_to_utc=True,
        fetch_historical_ohlcv_at_start: bool = False,
        fetch_historical_ohlcv_start_unix_timestamp_seconds: Optional[int] = None,
        fetch_historical_ohlcv_end_unix_timestamp_seconds: Optional[int] = None,
        keep_historical_ohlcv_seconds: Optional[int] = 300,  # max historical data time span
        remove_historical_ohlcv_interval_seconds: Optional[int] = 60,  # how often to remove
        # account
        is_paper_trading: bool = False,
        api_key: str = "",
        api_secret: str = "",
        api_passphrase: str = "",
        # order
        subscribe_order: bool = False,
        fetch_historical_order_at_start: bool = False,
        fetch_historical_order_start_unix_timestamp_seconds: Optional[int] = None,
        fetch_historical_order_end_unix_timestamp_seconds: Optional[int] = None,
        keep_historical_order_seconds: Optional[int] = 300,  # max historical data time span
        remove_historical_order_interval_seconds: Optional[int] = 60,  # how often to remove
        # fill
        subscribe_fill: bool = False,
        fetch_historical_fill_at_start: bool = False,
        fetch_historical_fill_start_unix_timestamp_seconds: Optional[int] = None,
        fetch_historical_fill_end_unix_timestamp_seconds: Optional[int] = None,
        keep_historical_fill_seconds: Optional[int] = 300,  # max historical data time span
        remove_historical_fill_interval_seconds: Optional[int] = 60,  # how often to remove
        # position
        subscribe_position: bool = False,
        # balance
        subscribe_balance: bool = False,
        # settings for using REST API to periodically sync data with the exchange
        rest_market_data_fetch_all_instrument_information_at_start: bool = True,
        rest_market_data_fetch_all_instrument_information_period_seconds: Optional[int] = 300,
        rest_market_data_fetch_bbo_period_seconds: Optional[int] = 300,
        rest_account_fetch_open_order_at_start: bool = True,
        rest_account_cancel_open_order_at_start: bool = False,
        rest_account_check_open_order_period_seconds: Optional[int] = 60,
        rest_account_check_open_order_threshold_seconds: Optional[int] = 60,
        rest_account_check_in_flight_order_period_seconds: Optional[int] = 10,
        rest_account_check_in_flight_order_threshold_seconds: Optional[int] = 10,
        rest_account_fetch_position_period_seconds: Optional[int] = 60,
        rest_account_fetch_balance_period_seconds: Optional[int] = 60,
        rest_market_data_send_consecutive_request_delay_seconds: Optional[
            float
        ] = 0.05,  # only applicable to paginated requests such as fetching historical data
        rest_account_send_consecutive_request_delay_seconds: Optional[float] = 0.05,  # only applicable to paginated requests such as fetching historical data
        # settings for using Websocket API to stream realtime data from the exchange
        websocket_connection_protocol_level_heartbeat_period_seconds: Optional[int] = 10,
        websocket_connection_application_level_heartbeat_period_seconds: Optional[int] = 10,
        websocket_connection_application_level_heartbeat_timeout_seconds: Optional[int] = 20,
        websocket_connection_auto_reconnect: bool = True,
        websocket_market_data_channel_symbols_limit: Optional[int] = 50,
        websocket_market_data_channel_send_consecutive_request_delay_seconds: Optional[
            float
        ] = 0.05,  # only applicable to divided requests such as subscribing on many symbols
        trade_api_method_preference: Optional[ApiMethod] = ApiMethod.REST,  # which API method is preferred to create/cancel orders
        extra_data: Any = None,  # arbitrary user-defined data
        start_wait_seconds: Optional[float] = 1,  # wait time at start
        stop_wait_seconds: Optional[float] = 1,  # wait time at stop
        json_serialize: Optional[Callable[[Any], str]] = None,  # function to serialize json
        json_deserialize: Optional[Callable[[str], Any]] = None,  # function to deserialize json
        logger: Optional[LoggerApi] = None,
        ssl: bool | aiohttp.Fingerprint | ssl.SSLContext = False,  # SSL validation mode. True for default SSL check
        # (ssl.create_default_context() is used), False for skip SSL certificate validation,
        # aiohttp.Fingerprint for fingerprint validation, ssl.SSLContext for custom SSL certificate validation.
    ) -> None:
    ...
```
* Key variables and methods.
```
class ExchangeApi:

    def __init__(self) -> None:

        self.all_instrument_information: Dict[Symbol, InstrumentInformation] = {}

        self.bbos: Dict[Symbol, Bbo] = {}

        self.trades: Dict[Symbol, List[Trade]] = {}  # the list of Trade objects are sorted earliest to latest

        self.ohlcvs: Dict[Symbol, List[Ohlcv]] = {}  # the list of Ohlcv objects are sorted earliest to latest

        self.orders: Dict[Symbol, List[Order]] = {}  # the list of Order objects are sorted earliest to latest

        self.fills: Dict[Symbol, List[Fill]] = {}  # the list of Fill objects are sorted earliest to latest

        self.positions: Dict[Symbol, Position] = {}

        self.balances: Dict[Symbol, Balance] = {}

    async def start(self) -> None:
        raise NotImplementedError

    async def stop(self) -> None:
        raise NotImplementedError

    async def create_order(self, *, order: Order, trade_api_method_preference: Optional[ApiMethod] = None) -> Order:
        raise NotImplementedError

    async def cancel_order(
        self,
        *,
        symbol: str,
        order_id: Optional[str] = None,
        client_order_id: Optional[str] = None,
        trade_api_method_preference: Optional[ApiMethod] = None,
        local_update_time_point: Optional[Tuple[int, int]] = None,
    ) -> None:
        raise NotImplementedError

    async def cancel_orders(
        self,
        *,
        symbol: Optional[str] = None,
        order_ids: Optional[Iterable[str]] = None,
        client_order_ids: Optional[Iterable[str]] = None,
        margin_asset: Optional[str] = None,
        trade_api_method_preference: Optional[ApiMethod] = None,
        local_update_time_point: Optional[Tuple[int, int]] = None,
    ) -> None:
        # if symbol is not provided, it will try to cancel all open orders
        raise NotImplementedError
```
* [Details](src/crypto_trade/exchange_api.py).

## Examples
* [Quick start](examples/quick_start.py).

## Thread Safety
* Single thread.

## Performance Tuning
* [Use a faster json library such as orjson](tests/test_orjson.py).
* [Use a faster event loop such as uvloop](tests/test_uvloop.py).
