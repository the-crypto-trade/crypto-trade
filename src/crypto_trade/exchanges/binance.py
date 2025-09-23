import hashlib
import hmac
from typing import Any

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

from crypto_trade.exchanges.delegates.binance_usds_margined_futures import BinanceUsdsMarginedFutures


class BinanceInstrumentType(StrEnum):
    SPOT = "spot"
    MARGIN = "margin" # only support cross margin
    USDS_MARGINED_FUTURES = "usds_margined_futures"
    COIN_MARGINED_FUTURES = "coin_margined_futures"

class Binance:

    def __init__(self, *, instrument_type: BinanceInstrumentType, **kwargs) -> None:
        # if instrument_type == BinanceInstrumentType.SPOT:
        #     self.delegate = BinanceSpot(**kwargs)
        # elif instrument_type == BinanceInstrumentType.MARGIN:
        #     self.delegate = BinanceMargin(**kwargs)
        # el
        if instrument_type == BinanceInstrumentType.USDS_MARGINED_FUTURES:
            self.delegate = BinanceUSDSFutures(**kwargs)
        # elif instrument_type == BinanceInstrumentType.COIN_MARGINED_FUTURES:
        #     self.delegate = BinanceCoinFutures(**kwargs)
        else:
            raise ValueError(f"Unsupported instrument_type {instrument_type} for exchange binance")

    def __getattr__(self, attr: Any) -> Any:
        return getattr(self.delegate, attr)
