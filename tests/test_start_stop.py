import asyncio
import sys
import traceback

from crypto_trade.exchanges.okx import Okx, OkxInstrumentType
from crypto_trade.utility import unix_timestamp_seconds_now


async def main():
    try:
        symbol = "BTC-USDT"
        now_unix_timestamp_seconds = unix_timestamp_seconds_now()
        exchange = Okx(
            instrument_type=OkxInstrumentType.SPOT,
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


def test_start_stop():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(main())
    try:
        loop.run_forever()
    except KeyboardInterrupt:
        loop.stop()


if __name__ == "__main__":
    test_start_stop()
