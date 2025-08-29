import asyncio
import csv
import os
import sys
import traceback
from datetime import datetime, timezone

from crypto_trade.exchanges.bybit import Bybit, BybitInstrumentType


async def main():
    try:
        symbol = os.getenv("SYMBOL", "BTCUSDT")

        def parse_env_timestamp(env_var, default="1970-01-01"):
            date_str = os.getenv(env_var, default)
            dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            return int(dt.timestamp())

        start_ts = parse_env_timestamp("START_TIME")
        end_ts = parse_env_timestamp("END_TIME")

        exchange = Bybit(
            instrument_type=BybitInstrumentType.SPOT,
            symbols={symbol},
            ohlcv_interval_seconds=60,
            fetch_historical_ohlcv_at_start=True,
            fetch_historical_ohlcv_start_unix_timestamp_seconds=start_ts,
            fetch_historical_ohlcv_end_unix_timestamp_seconds=end_ts,
            remove_historical_ohlcv_interval_seconds=None,  # do not remove any historical data
        )

        await exchange.start()
        await exchange.stop()

        if symbol in exchange.ohlcvs:
            with open(os.getenv("SAVE_DATA_FILE_PATH", f"data.csv"), "w", newline="") as f:
                writer = csv.writer(f)
                writer.writerow(["datetime", "open", "high", "low", "close", "volume", "quote_volume"])
                for ohlcv in exchange.ohlcvs[symbol]:
                    writer.writerow(
                        [
                            datetime.fromtimestamp(ohlcv.start_unix_timestamp_seconds, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
                            ohlcv.open_price,
                            ohlcv.high_price,
                            ohlcv.low_price,
                            ohlcv.close_price,
                            ohlcv.volume,
                            ohlcv.quote_volume,
                        ]
                    )

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


if __name__ == "__main__":
    asyncio.run(main())
