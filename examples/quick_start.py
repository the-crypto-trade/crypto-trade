import asyncio
import os
import pprint
import sys
import time
import traceback

from crypto_trade.exchange_api import ApiMethod, Order
from crypto_trade.exchanges.bybit import Bybit, BybitInstrumentType
from crypto_trade.utility import Logger, LogLevel


async def main():
    try:
        # Default log level is WARNING. Here is how to change it:
        logger = Logger(level=getattr(LogLevel, os.getenv("LOG_LEVEL", "WARNING")), name="bybit__spot")
        symbol = os.getenv("SYMBOL", "BTCUSDT")

        exchange = Bybit(
            instrument_type=BybitInstrumentType.SPOT,
            symbols={symbol},  # a comma-separated string or an iterable of strings. Use '*' to represent all symbols that are open for trade.
            subscribe_bbo=True,
            subscribe_order=True,
            subscribe_balance=True,
            is_paper_trading=False,  # https://www.bybit.com/en/help-center/article/How-to-Request-Test-Coins-on-Testnet
            api_key=os.getenv("BYBIT_API_KEY", ""),
            api_secret=os.getenv("BYBIT_API_SECRET", ""),
            logger=logger,
            start_wait_seconds=float(os.getenv("START_WAIT_SECONDS", "1")),  # increase this value if connecting through a slow VPN
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
                client_order_id=str(int(time.time() * 1000)),
                is_buy=os.getenv("SIDE", "BUY") == "BUY",
                price=exchange.bbos[symbol].best_ask_price,
                quantity="0.001",
                is_market=False,  # omit or set to True for limit order
                extra_params=(
                    {
                        "isLeverage": 1,
                    }
                    if os.getenv("IS_LEVERAGE")
                    else None
                ),  # extra parameter values to pass through to exchange-specific API
            )
        )

        await asyncio.sleep(float(os.getenv("SLEEP_SECONDS", "0.1")))  # increase this value if connecting through a slow VPN

        print("AFTER submitting order\n")
        print("bbos:")
        pprint.pp(exchange.bbos)
        print("\n")
        print("orders:")
        pprint.pp(exchange.orders)
        print("\n")
        print("balances:")
        pprint.pp(exchange.balances)
        print("\n")

        await exchange.stop()

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


if __name__ == "__main__":
    asyncio.run(main())
