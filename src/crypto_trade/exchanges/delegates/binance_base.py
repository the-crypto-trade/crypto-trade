import hashlib
import hmac

try:
    from enum import StrEnum
except ImportError:
    from strenum import StrEnum  # type: ignore

from crypto_trade.exchange_api import (
    ApiMethod,
    Balance,
    Bbo,
    Exchange,
    Fill,
    InstrumentInformation,
    Ohlcv,
    Order,
    OrderStatus,
    Position,
    Trade,
)
from crypto_trade.utility import (
    RestRequest,
    WebsocketRequest,
    convert_time_point_to_unix_timestamp_milliseconds,
    convert_unix_timestamp_milliseconds_to_time_point,
    normalize_decimal_string,
    remove_leading_negative_sign_in_string,
    time_point_now,
    unix_timestamp_milliseconds_now,
)


class BinanceBase(Exchange):

    def __init__(self, **kwargs) -> None:
        super().__init__(name="binance", **kwargs)
