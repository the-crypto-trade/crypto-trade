<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Crypto Trade](#crypto-trade)
  - [Branches And Tags](#branches-and-tags)
  - [Installation](#installation)
    - [Install From Github](#install-from-github)
    - [Install Locally](#install-locally)
  - [Examples](#examples)
  - [API](#api)
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
  * Many more coming soon.
* [Join the discussion](https://github.com/the-crypto-trade/crypto-trade/discussions).


## Branches And Tags
* The `main` branch may contain experimental features. Please use tagged releases for stability.

## Installation
* It's recommended that you install in a virtual environment of your choosing.

### Install From Github

    pip install git+https://github.com/the-crypto-trade/crypto-trade

### Install Locally

    pip install -e ".[dev]"

## Examples
* [Quick start](examples/quick_start.py).

## API
* Initialize. Commonly used options: `instrument_type`, `symbols`, `subscribe_bbo`, `subscribe_trade`, `subscribe_ohlcv`, `subscribe_order`, `subscribe_fill`, `subscribe_position`, `subscribe_balance`, `trade_api_method_preference`, `margin_asset`.
```
from crypto_trade.exchanges.bybit import Bybit, BybitInstrumentType

exchange = Bybit(
    instrument_type=BybitInstrumentType.SPOT,
    symbols={"BTCUSDT"},  # a comma-separated string or an iterable of strings. Use '*' to represent all symbols that are open for trade.
    subscribe_bbo=True,
    subscribe_order=True,
    subscribe_balance=True,
    api_key=...,
    api_secret=...,
)

await exchange.start()
```
* Access synced states at any time. Commonly used states: `bbos`, `trades`, `ohlcvs`, `orders`, `fills`, `positions`, `balances`.
```
import pprint
pprint.pp(exchange.bbos)
pprint.pp(exchange.orders)
pprint.pp(exchange.balances)
```
* You may also configure to fetch historical trades/ohlcvs/orders/fills at start time. For more details, see [here](src/crypto_trade/exchange_api.py).

## Thread Safety
* Single threaded based on Python's asyncio.

## Performance Tuning
* [Use a faster json library such as orjson](tests/test_orjson.py).
* [Use a faster event loop such as uvloop](tests/test_uvloop.py).
