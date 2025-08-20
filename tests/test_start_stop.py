import argparse
import asyncio
import sys
import traceback

import pytest

from crypto_trade.exchanges.bybit import Bybit, BybitInstrumentType
from crypto_trade.exchanges.okx import Okx, OkxInstrumentType
from crypto_trade.utility import unix_timestamp_seconds_now

EXCHANGE_CLASSES = {
    "Bybit": Bybit,
    "Okx": Okx,
}

EXCHANGE_INSTRUMENT_TYPES = {
    "Bybit": BybitInstrumentType,
    "Okx": OkxInstrumentType,
}


async def main(exchange_name, exchange_instrument_type_name, symbol):
    try:
        print(f"main: {exchange_name}, {exchange_instrument_type_name}, {symbol}")

        exchange_class = EXCHANGE_CLASSES.get(exchange_name)
        if not exchange_class:
            raise ValueError(f"Exchange class '{exchange_name}' not found")

        enum_class = EXCHANGE_INSTRUMENT_TYPES.get(exchange_name)
        if not enum_class:
            raise ValueError(f"Unsupported exchange: {exchange_name}")
        try:
            instrument_type_enum = enum_class[exchange_instrument_type_name.upper()]
        except KeyError:
            raise ValueError(f"Invalid instrument type '{exchange_instrument_type_name}' for {exchange_name}")

        now_unix_timestamp_seconds = unix_timestamp_seconds_now()
        exchange = exchange_class(
            instrument_type=instrument_type_enum,
            symbols={symbol},
            subscribe_bbo=True,
            subscribe_trade=True,
            fetch_historical_trade_at_start=True,
            fetch_historical_trade_start_unix_timestamp_seconds=now_unix_timestamp_seconds - 1,
            fetch_historical_trade_end_unix_timestamp_seconds=now_unix_timestamp_seconds,
            subscribe_ohlcv=True,
            fetch_historical_ohlcv_at_start=True,
            fetch_historical_ohlcv_start_unix_timestamp_seconds=now_unix_timestamp_seconds - 1,
            fetch_historical_ohlcv_end_unix_timestamp_seconds=now_unix_timestamp_seconds,
            subscribe_order=True,
            fetch_historical_order_at_start=True,
            fetch_historical_order_start_unix_timestamp_seconds=now_unix_timestamp_seconds - 1,
            fetch_historical_order_end_unix_timestamp_seconds=now_unix_timestamp_seconds,
            subscribe_fill=True,
            fetch_historical_fill_at_start=True,
            fetch_historical_fill_start_unix_timestamp_seconds=now_unix_timestamp_seconds - 1,
            fetch_historical_fill_end_unix_timestamp_seconds=now_unix_timestamp_seconds,
            subscribe_position=True,
            subscribe_balance=True,
        )

        await exchange.start()

        await exchange.stop()

        asyncio.get_running_loop().stop()

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


@pytest.mark.parametrize(
    "exchange, exchange_instrument_type, symbol",
    [
        ("Okx", "SPOT", "BTC-USDT"),
        ("Okx", "MARGIN", "BTC-USDT"),
        ("Okx", "SWAP", "BTC-USDT-SWAP"),
        ("Bybit", "SPOT", "BTCUSDT"),
        ("Bybit", "LINEAR", "BTCUSDT"),
        ("Bybit", "INVERSE", "BTCUSD"),
    ],
)
def test_start_stop(exchange, exchange_instrument_type, symbol):
    asyncio.run(main())


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--exchange_instrument_type", required=True)
    parser.add_argument("--symbol", required=True)

    args = parser.parse_args()

    test_start_stop(args.exchange, args.exchange_instrument_type, args.symbol)
