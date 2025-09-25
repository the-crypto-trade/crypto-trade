#!/usr/bin/env python3

import asyncio
import sys
import traceback

import orjson

from crypto_trade.exchanges.okx import Okx, OkxInstrumentType


# The output type of json_serialize is expected to be a string
# The output type of orjson.dumps is bytes
def json_serialize(data):
    return orjson.dumps(data).decode()  # pylint: disable=maybe-no-member


async def main():
    try:
        symbol = "BTC-USDT"
        exchange = Okx(
            instrument_type=OkxInstrumentType.SPOT,
            symbols={symbol},
            subscribe_bbo=True,
            subscribe_order=True,
            json_serialize=json_serialize,
            json_deserialize=orjson.loads,  # pylint: disable=maybe-no-member
        )

        await exchange.start()

        await exchange.stop()

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


def test_start_stop():
    asyncio.run(main())


if __name__ == "__main__":
    test_start_stop()
