<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [Crypto Trade](#crypto-trade)
  - [Branches](#branches)
  - [Installation](#installation)
    - [Install From PyPi](#install-from-pypi)
    - [Install Locally](#install-locally)
  - [Public API](#public-api)
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
  * [OKX](https://www.okx.com/join/47636709).
  * More coming soon...
* Public beta. All kinds of feedbacks and contributions are welcomed.

## Branches
* The `main` branch may contain experimental features.
* The `release` branch represents the most recent stable release.

## Installation
* It's recommended that you install in a virtual environment of your choosing.

### Install From Github

    pip install git+https://github.com/the-crypto-trade/crypto-trade

For developers:

    pip install "crypto_trade[dev] @ git+https://github.com/the-crypto-trade/crypto-trade"

### Install Locally

    pip install .

For developers:

    pip install '.[dev]'

## Public API
* [Initialize: see the constructor in class ExchangeBase.](src/crypto_trade/core/exchanges/exchange_base.py)
* [Call: see the methods in class ExchangeInterface.](src/crypto_trade/core/exchanges/exchange_interface.py)

## Examples
* [Quick start](examples/quick_start.py)

## Thread Safety
* Single thread.

## Performance Tuning
* [Use a faster json library such as orjson](tests/test_orjson.py).
* [Use a faster event loop such as uvloop](tests/test_uvloop.py).
