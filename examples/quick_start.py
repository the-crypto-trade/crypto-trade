import asyncio
import os
import pprint
import sys
import time
import traceback

from crypto_trade.exchange_api import Order
from crypto_trade.exchanges.okx import Okx, OkxInstrumentType


async def main():
    try:
        symbol = "BTC-USDT"
        instrument_type = OkxInstrumentType.SPOT
        exchange = Okx(
            instrument_type=instrument_type,
            symbols={symbol},  # a comma-separated string or an iterable of strings. Use '*' to represent all symbols that are open for trade.
            subscribe_bbo=True,
            subscribe_order=True,
            is_paper_trading=True,  # https://www.exchange.com/docs-v5/en/#overview-demo-trading-services
            api_key=os.getenv("OKX_API_KEY", ""),
            api_secret=os.getenv("OKX_API_SECRET", ""),
            api_passphrase=os.getenv("OKX_API_PASSPHRASE", ""),
        )

        await exchange.start()

        pprint.pp(exchange.bbos)
        print("\n")

        client_order_id = str(int(time.time() * 1000))

        await exchange.create_order(
            order=Order(
                symbol=symbol,
                client_order_id=client_order_id,
                is_buy=True,
                price="10000",
                quantity=exchange.all_instrument_information[symbol].order_quantity_min,
            )
        )

        await asyncio.sleep(1)

        pprint.pp(exchange.orders)
        print("\n")

        await exchange.cancel_order(symbol=symbol, client_order_id=client_order_id)

        await asyncio.sleep(1)

        pprint.pp(exchange.orders)

        await exchange.stop()
        asyncio.get_running_loop().stop()

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(main())
    loop.run_forever()
