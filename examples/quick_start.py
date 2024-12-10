import asyncio
import os
import pprint
import sys
import time
import traceback

from crypto_trade.exchange_api import Order
from crypto_trade.exchanges.okx import Okx, OkxInstrumentType

# from crypto_trade.utility.logger import Logger
# from crypto_trade.utility.logger_interface import LogLevel
# from crypto_trade.core.exchanges.exchange_api import Exchange
# from crypto_trade.core.exchanges.exchange_interface import MarginType
# from crypto_trade.core.exchanges.exchange_interface import ApiMethod


async def main():
    try:
        # Default log level is WARNING. Here is how to change it:
        # logger = Logger(name="crypto_trade", level=LogLevel.TRACE)
        # Exchange.set_logger(logger)

        symbol = "BTC-USDT"
        instrument_type = OkxInstrumentType.SPOT  # OkxInstrumentType.MARGIN
        margin_type = None  # MarginType.ISOLATED, MarginType.CROSS
        okx = Okx(
            instrument_type=instrument_type,
            symbols={symbol},  # a comma-separated string or an iterable of strings. Use '*' to represent all symbols that are open for trade.
            subscribe_bbo=True,
            # subscribe_trade=True,
            # fetch_historical_trade_at_start=True,
            # subscribe_ohlcv=True,
            # fetch_historical_ohlcv_at_start=True,
            subscribe_order=True,
            # fetch_historical_order_at_start=True,
            # subscribe_fill=True,
            # fetch_historical_fill_at_start=True,
            # subscribe_position=True,
            # subscribe_balance=True,
            is_demo_trading=True,  # https://www.okx.com/docs-v5/en/#overview-demo-trading-services
            api_key=os.environ.get("OKX_API_KEY", ""),
            api_secret=os.environ.get("OKX_API_SECRET", ""),
            api_passphrase=os.environ.get("OKX_API_PASSPHRASE", ""),
            margin_type=margin_type,
            # trade_api_method_preference = ApiMethod.WEBSOCKET,
        )

        await okx.start()

        pprint.pp(okx.bbos)
        print("\n")

        client_order_id = str(int(time.time() * 1000))
        await okx.create_order(
            order=Order(
                symbol=symbol,
                client_order_id=client_order_id,
                is_buy=True,
                price="10000",
                quantity=okx.all_instrument_information[symbol].order_quantity_min,
                # extra_params={"ccy": "USDT"},  # Extra parameters to pass through to the exchange's underlying API
            )
        )

        pprint.pp(okx.orders)
        print("\n")
        # If the websocket order update message comes before the rest order acknowledgement response, the printed
        # order status will be NEW, otherwise the order status will be CREATE_ACKNOWLEDGED.

        await asyncio.sleep(1)

        pprint.pp(okx.orders)
        print("\n")

        await okx.cancel_order(symbol=symbol, client_order_id=client_order_id)

        pprint.pp(okx.orders)
        print("\n")
        # If the websocket order update message comes before the rest order acknowledgement response, the printed
        # order status will be CANCELED, otherwise the order status will be CANCEL_ACKNOWLEDGED.

        await asyncio.sleep(1)

        pprint.pp(okx.orders)

        await okx.stop()
        asyncio.get_running_loop().stop()

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        asyncio.get_running_loop().stop()
