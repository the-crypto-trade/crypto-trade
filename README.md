<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Crypto Trade](#crypto-trade)
  - [Branches And Tags](#branches-and-tags)
  - [Installation](#installation)
    - [Use requirements.txt](#use-requirementstxt)
    - [Use pyproject.toml](#use-pyprojecttoml)
    - [Use pyproject.toml With Poetry](#use-pyprojecttoml-with-poetry)
    - [Install Directly From Github](#install-directly-from-github)
    - [Install Directly From Local Path](#install-directly-from-local-path)
  - [Examples](#examples)
  - [API](#api)
  - [Paper Trading](#paper-trading)
  - [Thread Safety](#thread-safety)
  - [Performance Tuning](#performance-tuning)
  - [Applications](#applications)
    - [Spot Market Making](#spot-market-making)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

# Crypto Trade
* A pure Python library for trading on cryptocurrency exchanges.
* More than a library, it syncs exchange data to local memory and keeps track of realtime data updates.
* Unified API for different exchanges.
* Super simple to use. Your code will be in a linear and synchronous manner. The library takes care of asynchronous networking.
* Supported exchanges:
  * [Binance](https://accounts.maxweb.black/register?ref=1116718520)
  * [Bybit](https://www.bybit.com/invite?ref=XNYP2K).
  * [Okx](https://www.okx.com/join/47636709).
  * Many more coming soon.
* A spot market making application is provided as an end-to-end solution for liquidity providers.
* [Join the discussion](https://github.com/the-crypto-trade/crypto-trade/discussions).
* Contact info: cryptotrade606@gmail.com.


## Branches And Tags
* The `main` branch may contain experimental features. Please use tagged releases for stability.

## Installation
* It is recommended that you install it in a virtual environment.

### Use requirements.txt

    git+https://github.com/the-crypto-trade/crypto-trade.git

### Use pyproject.toml

    [project]
    dependencies = [
        "crypto-trade @ git+https://github.com/the-crypto-trade/crypto-trade.git",
    ]

### Use pyproject.toml With Poetry

    [tool.poetry.dependencies]
    crypto-trade = { git = "https://github.com/the-crypto-trade/crypto-trade.git" }

### Install Directly From Github

    pip install git+https://github.com/the-crypto-trade/crypto-trade

### Install Directly From Local Path

    pip install /path/to/crypto-trade

## Examples
* [Quick start](examples/quick_start.py).
* [Download historical data](examples/download_historical_data.py).
* [Backtest](examples/backtest.py).

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
    is_paper_trading=False,
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
* You may also configure to fetch historical trades/ohlcvs/orders/fills. One example can be found at [Download historical data](examples/download_historical_data.py). For more details, see [here](src/crypto_trade/exchange_api.py). If you configure the library to both subscribe to real-time data and fetch historical data, it will first subscribe to real-time data, then fetch historical data in reverse chronological order, so that real-time and historical data join seamlessly without any gaps.

## Paper Trading
* When `is_paper_trading` is set to `True`, trading will be performed in a testing environment. See below for details.
* For [Binance](https://accounts.maxweb.black/register?ref=1116718520), its testnet environment will be used: https://testnet.binance.vision and https://testnet.binancefuture.com.
* For [Bybit](https://www.bybit.com/invite?ref=XNYP2K), its testnet environment will be used: https://testnet.bybit.com.
* For [Okx](https://www.okx.com/join/47636709), its demo trading environment will be used: https://www.okx.com/en-us/help/how-to-use-demo-trading.

## Thread Safety
* Single threaded based on Python's asyncio.

## Performance Tuning
* [Use a faster json library such as orjson](tests/test_orjson.py).
* [Use a faster event loop such as uvloop](tests/test_uvloop.py).

## Applications

### Spot Market Making
* Source code: [applications/market_making.py](applications/market_making.py)
* Configuration: see the beginning of the source code for environment variables and command line arguments.
* Quick start: `env API_KEY=... API_SECRET=... python3 applications/market_making.py --exchange-name bybit --base-assets BTC --quote-asset USDT`
