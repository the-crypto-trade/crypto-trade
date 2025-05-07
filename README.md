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
  * Many more coming soon.
* [Join the discussion](https://github.com/the-crypto-trade/crypto-trade/discussions) and [sign up for updates](https://sibforms.com/serve/MUIFAL9ivrWSOYW01XIFCAjBydHnMH1nN8LgeHYj54VuVb-TttFX9szkX7ROEWj3uyfkhJX3OK8W22BP1r_LRsIuaBI-c7TUVWkSU1WiYLMJgxehYxXERzv1Gb9pu2XcoaGG2QPGq4vmorm05f7FgezdIXmAzy3D6B00sYh_EZvZ6H-RMFzGizD7TWOrB8fxkUB1QdD2rABQAELd).


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

## Examples
* [Quick start](examples/quick_start.py).

## Thread Safety
* Single thread.

## Performance Tuning
* [Use a faster json library such as orjson](tests/test_orjson.py).
* [Use a faster event loop such as uvloop](tests/test_uvloop.py).
