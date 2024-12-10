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
        okx = Okx(
            instrument_type=OkxInstrumentType.SPOT,
            symbols={symbol},
            subscribe_bbo=True,
            subscribe_order=True,
            json_serialize=json_serialize,
            json_deserialize=orjson.loads,  # pylint: disable=maybe-no-member
        )

        await okx.start()

        await okx.stop()

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
