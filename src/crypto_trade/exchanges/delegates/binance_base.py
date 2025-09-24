
try:
    from enum import StrEnum
except ImportError:
    from strenum import StrEnum  # type: ignore

from crypto_trade.exchange_api import (
    Exchange,
)


class BinanceBase(Exchange):

    def __init__(self, **kwargs) -> None:
        super().__init__(name="binance", **kwargs)
