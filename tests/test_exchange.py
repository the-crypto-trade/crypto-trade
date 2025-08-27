import argparse
import asyncio
import sys
import traceback

import pytest

from crypto_trade.exchanges.bybit import Bybit, BybitInstrumentType
from crypto_trade.exchanges.okx import Okx, OkxInstrumentType

all_instruments = ["okx__spot__BTC-USDT", "bybit__spot__BTCUSDT"]


async def main(instrument):
    try:
        splitted = instrument.split("__")
        exchange_name = splitted[0]
        instrument_type_str = splitted[1]
        symbol = splitted[2]

        instrument_type = {
            "okx": OkxInstrumentType,
            "bybit": BybitInstrumentType,
        }[
            exchange_name
        ][instrument_type_str.upper()]

        exchange_class = {
            "okx": Okx,
            "bybit": Bybit,
        }[exchange_name]

        exchange = exchange_class(
            instrument_type=instrument_type,
            symbols={symbol},
            subscribe_bbo=True,
            subscribe_order=True,
        )

        await exchange.start()

        await exchange.stop()

        asyncio.get_running_loop().stop()

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


@pytest.mark.parametrize("instrument", all_instruments)
def test_start_stop(instrument):
    asyncio.run(main(instrument))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--instruments",
        type=str,
    )
    args = parser.parse_args()
    instruments = args.instruments.split(",") if args.instruments else all_instruments
    for instrument in instruments:
        test_start_stop(instrument)
