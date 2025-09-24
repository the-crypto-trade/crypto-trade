#!/usr/bin/env python3

import asyncio
import os
import pprint
import sys
import time
import traceback

from crypto_trade.exchange_api import ApiMethod, Order
from crypto_trade.exchanges.binance import Binance, BinanceInstrumentType
from crypto_trade.utility import Logger, LogLevel


async def main():
    try:
        # Default log level is WARNING. Here is how to change it:
        logger = Logger(level=getattr(LogLevel, os.getenv("LOG_LEVEL", "WARNING")), name="")
        symbol = os.getenv("SYMBOL", "BTCUSDT")

        exchange = Binance(
            instrument_type=BinanceInstrumentType.USDS_MARGINED_FUTURES,
            symbols={symbol},  # a comma-separated string or an iterable of strings. Use '*' to represent all symbols that are open for trade.
            subscribe_bbo=True,
            subscribe_order=True,
            subscribe_position=True,
            subscribe_balance=True,
            is_paper_trading=False,  # https://www.bybit.com/en/help-center/article/How-to-Request-Test-Coins-on-Testnet
            api_key=os.getenv("BINANCE_API_KEY", ""),
            api_secret=os.getenv("BINANCE_API_SECRET", ""),
            logger=logger,
            start_wait_seconds=float(os.getenv("START_WAIT_SECONDS", "2")),  # increase this value if connecting through a slow VPN
            trade_api_method_preference=ApiMethod.REST,  # allowed values are ApiMethod.REST and ApiMethod.WEBSOCKET
        )

        await exchange.start()

        print("BEFORE submitting order\n")
        print("bbos:")
        pprint.pp(exchange.bbos)
        print("\n")
        print("orders:")
        pprint.pp(exchange.orders)
        print("\n")
        print("balances:")
        pprint.pp(exchange.balances)
        print("\n")

        await exchange.create_order(
            order=Order(
                symbol=symbol,
                client_order_id=exchange.generate_next_client_order_id(),
                is_buy=os.getenv("SIDE", "BUY") == "BUY",
                price='114000',
                quantity="0.001",
                is_market=False,  # omit or set to True for limit order
            )
        )

        await asyncio.sleep(float(os.getenv("SLEEP_SECONDS", "0.2")))  # increase this value if connecting through a slow VPN

        print("AFTER submitting order\n")
        print("bbos:")
        pprint.pp(exchange.bbos)
        print("\n")
        print("orders:")
        pprint.pp(exchange.orders)
        print("\n")
        print("positions:")
        pprint.pp(exchange.positions)
        print("\n")
        print("balances:")
        pprint.pp(exchange.balances)
        print("\n")

        await exchange.stop()
        # exchange is no longer useable after being stopped
        # if you need to use the exchange again, please recreate it

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


if __name__ == "__main__":
    asyncio.run(main())
