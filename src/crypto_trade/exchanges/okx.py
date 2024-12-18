import asyncio
import base64
import hashlib
import hmac
from datetime import datetime, timezone
from decimal import Decimal

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
    MarginType,
    Ohlcv,
    Order,
    OrderStatus,
    Position,
    Trade,
)
from crypto_trade.utility import (
    RestRequest,
    WebsocketRequest,
    convert_set_to_subsets,
    convert_unix_timestamp_milliseconds_to_time_point,
    normalize_decimal_string,
    remove_leading_negative_sign_in_string,
    time_point_now,
)


class OkxInstrumentType(StrEnum):
    SPOT = "SPOT"
    MARGIN = "MARGIN"
    SWAP = "SWAP"
    FUTURES = "FUTURES"
    OPTION = "OPTION"


class Okx(Exchange):

    def __init__(self, **kwargs) -> None:
        super().__init__(name="okx", **kwargs)

        self.rest_market_data_base_url = "https://www.okx.com"
        self.rest_account_base_url = self.rest_market_data_base_url
        self.rest_market_data_fetch_all_instrument_information_path = "/api/v5/public/instruments"
        self.rest_market_data_fetch_bbo_path = "/api/v5/market/tickers"
        self.rest_market_data_fetch_historical_trade_path = "/api/v5/market/history-trades"
        self.rest_market_data_fetch_historical_ohlcv_path = "/api/v5/market/history-candles"
        self.rest_account_create_order_path = "/api/v5/trade/order"
        self.rest_account_cancel_order_path = "/api/v5/trade/cancel-order"
        self.rest_account_fetch_order_path = "/api/v5/trade/order"
        self.rest_account_fetch_open_order_path = "/api/v5/trade/orders-pending"
        self.rest_account_fetch_position_path = "/api/v5/account/positions"
        self.rest_account_fetch_balance_path = "/api/v5/account/balance"
        self.rest_account_fetch_historical_order_path = "/api/v5/trade/orders-history"
        self.rest_account_fetch_historical_order_path_2 = "/api/v5/trade/orders-history-archive"
        self.rest_account_fetch_historical_fill_path = "/api/v5/trade/fills"
        self.rest_account_fetch_historical_fill_path_2 = "/api/v5/trade/fills-history"

        self.websocket_market_data_base_url = "wss://ws.okx.com:8443"
        if self.is_demo_trading:
            self.websocket_market_data_base_url = "wss://wspap.okx.com:8443"
        self.websocket_account_base_url = self.websocket_market_data_base_url
        self.websocket_market_data_path = "/ws/v5/public"
        self.websocket_market_data_path_2 = "/ws/v5/business"
        self.websocket_market_data_channel_bbo = "bbo-tbt"
        self.websocket_market_data_channel_trade = "trades"
        self.websocket_market_data_channel_ohlcv = "candle"
        self.websocket_account_path = "/ws/v5/private"
        self.websocket_account_channel_order = "orders"
        self.websocket_account_channel_position = "positions"
        self.websocket_account_channel_balance = "balance_and_position"
        self.websocket_account_trade_base_url = self.websocket_account_base_url
        self.websocket_account_trade_path = self.websocket_account_path

        self.order_status_mapping = {
            "canceled": OrderStatus.CANCELED,
            "live": OrderStatus.NEW,
            "partially_filled": OrderStatus.PARTIALLY_FILLED,
            "filled": OrderStatus.FILLED,
            "mmp_canceled": OrderStatus.CANCELED,
        }

        self.broker_id = "9cbc6a17a1fcBCDE"

        if self.instrument_type == OkxInstrumentType.SPOT:
            self.subscribe_position = False
            self.rest_account_fetch_position_period_seconds = None

    def is_instrument_type_valid(self, *, instrument_type):
        return instrument_type in {
            OkxInstrumentType.SPOT,
            OkxInstrumentType.MARGIN,
            OkxInstrumentType.SWAP,
            OkxInstrumentType.FUTURES,
            OkxInstrumentType.OPTION,
        }

    def sign_request(self, *, rest_request, time_point):
        if rest_request.headers is None:
            rest_request.headers = {}

        headers = rest_request.headers
        headers["CONTENT-TYPE"] = "application/json"
        headers["OK-ACCESS-KEY"] = self.api_key
        headers["OK-ACCESS-TIMESTAMP"] = (
            f"{datetime.fromtimestamp(time_point[0], tz=timezone.utc).strftime('%Y-%m-%dT%H:%M:%S')}.{str(time_point[1] // 1_000_000).zfill(3)}Z"
        )
        headers["OK-ACCESS-PASSPHRASE"] = self.api_passphrase
        headers["OK-ACCESS-SIGN"] = base64.b64encode(
            hmac.new(
                bytes(self.api_secret, "utf-8"),
                bytes(f"{headers['OK-ACCESS-TIMESTAMP']}{rest_request.method}{rest_request.path_with_query_string}{rest_request.payload or ''}", "utf-8"),
                digestmod=hashlib.sha256,
            ).digest()
        ).decode("utf-8")

        if self.is_demo_trading:
            headers["x-simulated-trading"] = "1"

    def rest_market_data_fetch_all_instrument_information_create_rest_request_function(self):
        return self.rest_market_data_create_get_request_function(
            path=self.rest_market_data_fetch_all_instrument_information_path, query_params={"instType": f"{self.instrument_type}"}
        )

    def rest_market_data_fetch_bbo_create_rest_request_function(self):

        return self.rest_market_data_create_get_request_function(
            path=self.rest_market_data_fetch_bbo_path,
            query_params={"instType": f"{OkxInstrumentType.SPOT if self.instrument_type ==OkxInstrumentType.MARGIN else self.instrument_type}"},
        )

    def rest_market_data_fetch_historical_trade_create_rest_request_function(self, *, symbol):
        query_params = {"instId": symbol, "type": 1}
        if self.rest_market_data_fetch_historical_trade_limit:
            query_params["limit"] = self.rest_market_data_fetch_historical_trade_limit

        return self.rest_market_data_create_get_request_function(path=self.rest_market_data_fetch_historical_trade_path, query_params=query_params)

    def rest_market_data_fetch_historical_ohlcv_create_rest_request_function(self, *, symbol):
        query_params = {
            "instId": symbol,
            "after": (
                self.fetch_historical_ohlcv_end_unix_timestamp_seconds // self.ohlcv_interval_seconds * self.ohlcv_interval_seconds
                + self.ohlcv_interval_seconds
            )
            * 1000,
            "bar": self.convert_ohlcv_interval_seconds_to_string(ohlcv_interval_seconds=self.ohlcv_interval_seconds),
        }
        if self.rest_market_data_fetch_historical_ohlcv_limit:
            query_params["limit"] = self.rest_market_data_fetch_historical_ohlcv_limit

        return self.rest_market_data_create_get_request_function(path=self.rest_market_data_fetch_historical_ohlcv_path, query_params=query_params)

    def rest_account_create_order_create_rest_request_function(self, *, order):
        return self.rest_account_create_post_request_function_with_signature(
            path=self.rest_account_create_order_path,
            json_payload=self.account_create_order_create_json_payload(order=order),
            json_serialize=self.json_serialize,
        )

    def rest_account_cancel_order_create_rest_request_function(self, *, symbol, order_id=None, client_order_id=None):
        return self.rest_account_create_post_request_function_with_signature(
            path=self.rest_account_cancel_order_path,
            json_payload=self.account_cancel_order_create_json_payload(symbol=symbol, order_id=order_id, client_order_id=client_order_id),
            json_serialize=self.json_serialize,
        )

    def rest_account_fetch_order_create_rest_request_function(self, *, symbol, order_id=None, client_order_id=None):
        query_params = {"instId": symbol}
        if order_id:
            query_params["ordId"] = order_id
        else:
            query_params["clOrdId"] = client_order_id
        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_order_path, query_params=query_params)

    def rest_account_fetch_open_order_create_rest_request_function(self):
        return self.rest_account_create_get_request_function_with_signature(
            path=self.rest_account_fetch_open_order_path, query_params={"instType": f"{self.instrument_type}"}
        )

    def rest_account_fetch_position_create_rest_request_function(self):
        return self.rest_account_create_get_request_function_with_signature(
            path=self.rest_account_fetch_position_path, query_params={"instType": f"{self.instrument_type}"}
        )

    def rest_account_fetch_balance_create_rest_request_function(self):
        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_balance_path)

    def rest_account_fetch_historical_order_create_rest_request_function(self, *, symbol):
        query_params = {"instType": f"{self.instrument_type}", "instId": symbol}
        if self.rest_account_fetch_historical_order_limit:
            query_params["limit"] = self.rest_account_fetch_historical_order_limit

        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_historical_order_path, query_params=query_params)

    def rest_account_fetch_historical_fill_create_rest_request_function(self, *, symbol):
        query_params = {"instType": f"{self.instrument_type}", "instId": symbol}
        if self.rest_account_fetch_historical_fill_limit:
            query_params["limit"] = self.rest_account_fetch_historical_fill_limit

        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_historical_fill_path, query_params=query_params)

    def is_rest_response_success(self, *, rest_response):
        return (
            super().is_rest_response_success(rest_response=rest_response)
            and rest_response.json_deserialized_payload
            and rest_response.json_deserialized_payload["code"] == "0"
        )

    def is_rest_response_for_all_instrument_information(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_market_data_fetch_all_instrument_information_path

    def is_rest_response_for_bbo(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_market_data_fetch_bbo_path

    def is_rest_response_for_historical_trade(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_market_data_fetch_historical_trade_path

    def is_rest_response_for_historical_ohlcv(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_market_data_fetch_historical_ohlcv_path

    def is_rest_response_for_create_order(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_create_order_path and rest_response.rest_request.method == RestRequest.METHOD_POST

    def is_rest_response_for_cancel_order(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_cancel_order_path

    def is_rest_response_for_fetch_order(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_order_path and rest_response.rest_request.method == RestRequest.METHOD_GET

    def is_rest_response_for_fetch_open_order(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_open_order_path

    def is_rest_response_for_fetch_position(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_position_path

    def is_rest_response_for_fetch_balance(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_balance_path

    def is_rest_response_for_historical_order(self, *, rest_response):
        return (
            rest_response.rest_request.path == self.rest_account_fetch_historical_order_path
            or rest_response.rest_request.path == self.rest_account_fetch_historical_order_path_2
        )

    def is_rest_response_for_historical_fill(self, *, rest_response):
        return (
            rest_response.rest_request.path == self.rest_account_fetch_historical_fill_path
            or rest_response.rest_request.path == self.rest_account_fetch_historical_fill_path_2
        )

    def convert_rest_response_for_all_instrument_information(self, *, json_deserialized_payload, rest_request):
        return [
            InstrumentInformation(
                api_method=ApiMethod.REST,
                symbol=x["instId"],
                base_asset=x["baseCcy"],
                quote_asset=x["quoteCcy"],
                order_price_increment=normalize_decimal_string(input=x["tickSz"]),
                order_quantity_increment=normalize_decimal_string(input=x["lotSz"]),
                order_quantity_min=normalize_decimal_string(input=x["minSz"]),
                order_quantity_max=normalize_decimal_string(input=x["maxLmtSz"]),
                order_quote_quantity_max=normalize_decimal_string(input=x["maxLmtAmt"]),
                margin_asset=x["settleCcy"],
                underlying_symbol=x["uly"],
                contract_size=normalize_decimal_string(input=x["ctVal"]),
                contract_multiplier=normalize_decimal_string(input=x["ctMult"]),
                expiry_time=int(expTime) // 1000 if (expTime := x["expTime"]) else None,
                is_open_for_trade=x["state"] in {"live", "preopen"},
            )
            for x in json_deserialized_payload["data"]
        ]

    def convert_rest_response_for_bbo(self, *, json_deserialized_payload, rest_request):
        return [
            Bbo(
                api_method=ApiMethod.REST,
                symbol=inst_id,
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
                best_bid_price=x.get("bidPx"),
                best_bid_size=x.get("bidSz"),
                best_ask_price=x.get("askPx"),
                best_ask_size=x.get("askSz"),
            )
            for x in json_deserialized_payload["data"]
            if (inst_id := x["instId"]) in self.symbols
        ]

    def convert_rest_response_for_historical_trade(self, *, json_deserialized_payload, rest_request):
        return [self.convert_dict_to_trade(input=x, api_method=ApiMethod.REST, symbol=x["instId"]) for x in json_deserialized_payload["data"]]

    def convert_rest_response_for_historical_trade_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        data = json_deserialized_payload["data"]

        if data:
            head = data[0]
            head_exchange_update_time_point = convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=head["ts"])
            head_trade_id_as_int = int(head["tradeId"])
            tail = data[-1]
            tail_exchange_update_time_point = convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=tail["ts"])
            tail_trade_id_as_int = int(tail["tradeId"])

            if (head_exchange_update_time_point, head_trade_id_as_int) < (tail_exchange_update_time_point, tail_trade_id_as_int):
                after = head_trade_id_as_int
                exchange_update_time_point = head_exchange_update_time_point
            else:
                after = tail_trade_id_as_int

                exchange_update_time_point = tail_exchange_update_time_point
            if (
                self.fetch_historical_trade_start_unix_timestamp_seconds is None
                or exchange_update_time_point[0] >= self.fetch_historical_trade_start_unix_timestamp_seconds
            ):
                query_params = {"instId": head["instId"], "type": 1, "after": after}
                if self.rest_market_data_fetch_historical_trade_limit:
                    query_params["limit"] = self.rest_market_data_fetch_historical_trade_limit

                return self.rest_market_data_create_get_request_function(path=self.rest_market_data_fetch_historical_trade_path, query_params=query_params)

    def convert_rest_response_for_historical_ohlcv(self, *, json_deserialized_payload, rest_request):
        inst_id = rest_request.query_params["instId"]

        return [self.convert_dict_to_ohlcv(input=x, api_method=ApiMethod.REST, symbol=inst_id) for x in json_deserialized_payload["data"]]

    def convert_rest_response_for_historical_ohlcv_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        data = json_deserialized_payload["data"]

        if data:
            head = data[0]
            head_ts = int(head[0])
            tail = data[-1]
            tail_ts = int(tail[0])

            if head_ts < tail_ts:
                after = head_ts
            else:
                after = tail_ts

            if self.fetch_historical_ohlcv_start_unix_timestamp_seconds is None or after // 1000 >= self.fetch_historical_ohlcv_start_unix_timestamp_seconds:
                query_params = {
                    "instId": rest_request.query_params["instId"],
                    "after": after,
                    "bar": self.convert_ohlcv_interval_seconds_to_string(ohlcv_interval_seconds=self.ohlcv_interval_seconds),
                }
                if self.rest_market_data_fetch_historical_ohlcv_limit:
                    query_params["limit"] = self.rest_market_data_fetch_historical_ohlcv_limit

                return self.rest_market_data_create_get_request_function(path=self.rest_market_data_fetch_historical_ohlcv_path, query_params=query_params)

    def convert_rest_response_for_create_order(self, *, json_deserialized_payload, rest_request):
        x = json_deserialized_payload["data"][0]

        return Order(
            api_method=ApiMethod.REST,
            symbol=rest_request.json_payload["instId"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
            order_id=x["ordId"],
            client_order_id=rest_request.json_payload.get("clOrdId"),
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
            status=OrderStatus.CREATE_ACKNOWLEDGED,
        )

    def convert_rest_response_for_cancel_order(self, *, json_deserialized_payload, rest_request):
        x = json_deserialized_payload["data"][0]

        return Order(
            api_method=ApiMethod.REST,
            symbol=rest_request.json_payload["instId"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
            order_id=rest_request.json_payload.get("ordId"),
            client_order_id=rest_request.json_payload.get("clOrdId"),
            status=OrderStatus.CANCEL_ACKNOWLEDGED,
        )

    def convert_rest_response_for_fetch_order(self, *, json_deserialized_payload, rest_request):
        x = json_deserialized_payload["data"][0]

        return self.convert_dict_to_order(input=x, api_method=ApiMethod.REST, symbol=x["instId"])

    def convert_rest_response_for_fetch_open_order(self, *, json_deserialized_payload, rest_request):
        return [self.convert_dict_to_order(input=x, api_method=ApiMethod.REST, symbol=x["instId"]) for x in json_deserialized_payload["data"]]

    def convert_rest_response_for_fetch_open_order_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        data = json_deserialized_payload["data"]

        if data:
            head = data[0]
            head_ord_id = head["ordId"]
            tail = data[-1]
            tail_ord_id = tail["ordId"]

            if head_ord_id < tail_ord_id:
                after = head_ord_id
            else:
                after = tail_ord_id
            query_params = {"instType": f"{self.instrument_type}", "after": after}

            if self.rest_account_fetch_open_order_limit:
                query_params["limit"] = self.rest_account_fetch_open_order_limit

            return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_open_order_path, query_params=query_params)

    def convert_rest_response_for_fetch_position(self, *, json_deserialized_payload, rest_request):
        return [self.convert_dict_to_position(input=x, api_method=ApiMethod.REST) for x in json_deserialized_payload["data"]]

    def convert_rest_response_for_fetch_balance(self, *, json_deserialized_payload, rest_request):
        return [self.convert_dict_to_balance(input=x, api_method=ApiMethod.REST) for x in json_deserialized_payload["data"][0]["details"]]

    def convert_rest_response_for_historical_order(self, *, json_deserialized_payload, rest_request):
        inst_id = rest_request.query_params["instId"]

        return [self.convert_dict_to_order(input=x, api_method=ApiMethod.REST, symbol=inst_id) for x in json_deserialized_payload["data"]]

    def convert_rest_response_for_historical_order_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        data = json_deserialized_payload["data"]

        if data:
            head = data[0]
            head_exchange_create_time_point = convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=head["cTime"])
            head_order_id_as_int = int(head["ordId"])
            tail = data[-1]
            tail_exchange_create_time_point = convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=tail["cTime"])
            tail_order_id_as_int = int(tail["ordId"])

            if (head_exchange_create_time_point, head_order_id_as_int) < (tail_exchange_create_time_point, tail_order_id_as_int):
                after = head_order_id_as_int
                exchange_create_time_point = head_exchange_create_time_point
            else:
                after = tail_order_id_as_int
                exchange_create_time_point = tail_exchange_create_time_point

            if (
                self.fetch_historical_order_start_unix_timestamp_seconds is None
                or exchange_create_time_point[0] >= self.fetch_historical_order_start_unix_timestamp_seconds
            ):
                return self.rest_account_create_get_request_function_with_signature(
                    path=rest_request.path, query_params={"instType": f"{self.instrument_type}", "instId": rest_request.query_params["instId"], "after": after}
                )
        elif rest_request.path == self.rest_account_fetch_historical_order_path:
            query_params = {"instType": f"{self.instrument_type}", "instId": rest_request.query_params["instId"]}

            if "after" in rest_request.query_params:
                query_params["after"] = rest_request.query_params["after"]
            if self.rest_account_fetch_historical_order_limit:
                query_params["limit"] = self.rest_account_fetch_historical_order_limit

            return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_historical_order_path_2, query_params=query_params)

    def convert_rest_response_for_historical_fill(self, *, json_deserialized_payload, rest_request):
        inst_id = rest_request.query_params["instId"]

        return [self.convert_dict_to_fill(input=x, api_method=ApiMethod.REST, symbol=inst_id) for x in json_deserialized_payload["data"]]

    def convert_rest_response_for_historical_fill_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        data = json_deserialized_payload["data"]

        if data:
            head = data[0]
            head_exchange_update_time_point = convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=head["fillTime"])
            head_bill_id_as_int = int(head["billId"])
            tail = data[-1]
            tail_exchange_update_time_point = convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=tail["fillTime"])
            tail_bill_id_as_int = int(tail["billId"])

            if (head_exchange_update_time_point, head_bill_id_as_int) < (tail_exchange_update_time_point, tail_bill_id_as_int):
                after = head_bill_id_as_int
                exchange_update_time_point = head_exchange_update_time_point
            else:
                after = tail_bill_id_as_int
                exchange_update_time_point = tail_exchange_update_time_point

            if (
                self.fetch_historical_fill_start_unix_timestamp_seconds is None
                or exchange_update_time_point[0] >= self.fetch_historical_fill_start_unix_timestamp_seconds
            ):
                return self.rest_account_create_get_request_function_with_signature(
                    path=rest_request.path, query_params={"instType": f"{self.instrument_type}", "instId": rest_request.query_params["instId"], "after": after}
                )
        elif rest_request.path == self.rest_account_fetch_historical_fill_path:
            query_params = {"instType": f"{self.instrument_type}", "instId": rest_request.query_params["instId"]}

            if "after" in rest_request.query_params:
                query_params["after"] = rest_request.query_params["after"]
            if self.rest_account_fetch_historical_fill_limit:
                query_params["limit"] = self.rest_account_fetch_historical_fill_limit

            return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_historical_fill_path_2, query_params=query_params)

    async def handle_rest_response_for_error(self, *, rest_response):
        self.logger.warning("rest_response", rest_response)

        if self.is_rest_response_for_create_order(rest_response=rest_response) or self.is_rest_response_for_cancel_order(rest_response=rest_response):
            await self.rest_account_fetch_order(
                symbol=rest_response.rest_request.json_payload["instId"],
                order_id=rest_response.rest_request.json_payload.get("ordId"),
                client_order_id=rest_response.rest_request.json_payload.get("clOrdId"),
            )
        elif self.is_rest_response_for_fetch_order(rest_response=rest_response):
            if (
                rest_response.status_code == 200
                and rest_response.json_deserialized_payload
                and rest_response.json_deserialized_payload.get("code") in {"51001", "51603"}
            ):
                self.replace_order(
                    symbol=rest_response.rest_request.query_params["instId"],
                    order_id=rest_response.rest_request.query_params.get("ordId"),
                    client_order_id=rest_response.rest_request.query_params.get("clOrdId"),
                    exchange_update_time_point=time_point_now(),
                    status=OrderStatus.REJECTED,
                )

    async def websocket_market_data_connect(self):
        if self.symbols:

            if self.subscribe_bbo or self.subscribe_trade:
                self.create_task(
                    coro=self.start_websocket_connect(
                        base_url=self.websocket_market_data_base_url,
                        path=self.websocket_market_data_path,
                        query_params=self.websocket_market_data_query_params,
                    )
                )

            if self.subscribe_ohlcv:
                self.create_task(
                    coro=self.start_websocket_connect(
                        base_url=self.websocket_market_data_base_url,
                        path=self.websocket_market_data_path_2,
                        query_params=self.websocket_market_data_query_params,
                    )
                )

    async def websocket_market_data_subscribe_for_bbo_trade(self, *, websocket_connection):
        symbols_subsets = convert_set_to_subsets(input=self.symbols, subset_length=self.websocket_market_data_channel_symbols_limit)
        for symbols_subset in symbols_subsets:
            await self.send_websocket_request(
                websocket_connection=websocket_connection,
                websocket_request=self.websocket_market_data_update_subscribe_create_websocket_request_for_bbo_trade(symbols=symbols_subset, is_subscribe=True),
            )
            if self.websocket_market_data_channel_send_consecutive_request_delay_seconds:
                await asyncio.sleep(self.websocket_market_data_channel_send_consecutive_request_delay_seconds)

    async def websocket_market_data_subscribe_for_ohlcv(self, *, websocket_connection):
        symbols_subsets = convert_set_to_subsets(input=self.symbols, subset_length=self.websocket_market_data_channel_symbols_limit)
        for symbols_subset in symbols_subsets:
            await self.send_websocket_request(
                websocket_connection=websocket_connection,
                websocket_request=self.websocket_market_data_update_subscribe_create_websocket_request_for_ohlcv(symbols=symbols_subset, is_subscribe=True),
            )
            if self.websocket_market_data_channel_send_consecutive_request_delay_seconds:
                await asyncio.sleep(self.websocket_market_data_channel_send_consecutive_request_delay_seconds)

    def websocket_connection_ping_on_application_level_create_websocket_request(self):
        payload = "ping"
        return self.websocket_create_request(payload=payload)

    def websocket_login_create_websocket_request(self, *, time_point):
        arg = {}
        arg["apiKey"] = self.api_key
        arg["passphrase"] = self.api_passphrase
        arg["timestamp"] = time_point[0]
        arg["sign"] = base64.b64encode(
            hmac.new(bytes(self.api_secret, "utf-8"), bytes(f"{arg['timestamp']}GET/users/self/verify", "utf-8"), digestmod=hashlib.sha256).digest()
        ).decode("utf-8")
        payload = self.json_serialize(
            {
                "op": "login",
                "args": [arg],
            }
        )
        return self.websocket_create_request(payload=payload)

    def websocket_market_data_update_subscribe_create_websocket_request_for_bbo_trade(self, *, symbols, is_subscribe):
        args = []

        for symbol in symbols:
            if self.subscribe_bbo:
                args.append({"channel": self.websocket_market_data_channel_bbo, "instId": symbol})
            if self.subscribe_trade:
                args.append({"channel": self.websocket_market_data_channel_trade, "instId": symbol})

        payload = self.json_serialize({"op": "subscribe", "args": args})
        return self.websocket_create_request(payload=payload)

    def websocket_market_data_update_subscribe_create_websocket_request_for_ohlcv(self, *, symbols, is_subscribe):
        args = []

        for symbol in self.symbols:
            args.append(
                {
                    "channel": self.websocket_market_data_channel_ohlcv
                    + self.convert_ohlcv_interval_seconds_to_string(ohlcv_interval_seconds=self.ohlcv_interval_seconds),
                    "instId": symbol,
                }
            )

        payload = self.json_serialize({"op": "subscribe", "args": args})
        return self.websocket_create_request(payload=payload)

    def websocket_account_update_subscribe_create_websocket_request(self, *, is_subscribe):
        args = []

        if self.subscribe_order or self.subscribe_fill:
            args.append(
                {
                    "channel": self.websocket_account_channel_order,
                    "instType": f"{self.instrument_type}",
                }
            )

        if self.subscribe_position:
            args.append(
                {
                    "channel": self.websocket_account_channel_position,
                    "instType": f"{self.instrument_type}",
                }
            )

        if self.subscribe_balance:
            args.append(
                {
                    "channel": self.websocket_account_channel_balance,
                    "instType": f"{self.instrument_type}",
                }
            )

        payload = self.json_serialize({"op": "subscribe", "args": args})
        return self.websocket_create_request(payload=payload)

    def websocket_account_create_order_create_websocket_request(self, *, order):
        id = self.generate_next_websocket_request_id()
        arg = self.account_create_order_create_json_payload(order=order)
        return WebsocketRequest(id=id, json_payload={"id": id, "op": "order", "args": [arg]}, json_serialize=self.json_serialize)

    def websocket_account_cancel_order_create_websocket_request(self, *, symbol, order_id=None, client_order_id=None):
        id = self.generate_next_websocket_request_id()
        arg = self.account_cancel_order_create_json_payload(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
        return WebsocketRequest(id=id, json_payload={"id": id, "op": "cancel-order", "args": [arg]}, json_serialize=self.json_serialize)

    async def websocket_on_message(self, *, websocket_connection, raw_websocket_message_data):
        # "pong" isn't valid json, only "\"pong\"" is valid json
        if raw_websocket_message_data != "pong":
            await super().websocket_on_message(websocket_connection=websocket_connection, raw_websocket_message_data=raw_websocket_message_data)

    def websocket_on_message_extract_data(self, *, websocket_message):
        json_deserialized_payload = websocket_message.json_deserialized_payload

        websocket_message.payload_summary = {
            "event": json_deserialized_payload.get("event"),
            "op": json_deserialized_payload.get("op"),
            "channel": json_deserialized_payload.get("arg", {}).get("channel"),
            "code": json_deserialized_payload.get("code"),
        }

        id = json_deserialized_payload.get("id")
        websocket_message.websocket_request_id = str(id) if id is not None else None

        if websocket_message.websocket_request_id:
            websocket_message.websocket_request = self.websocket_requests.get(websocket_message.websocket_request_id)

        return websocket_message

    def is_websocket_push_data(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["event"] is None and payload_summary["op"] is None

    def is_websocket_push_data_for_bbo(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["channel"] == self.websocket_market_data_channel_bbo

    def is_websocket_push_data_for_trade(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["channel"] == self.websocket_market_data_channel_trade

    def is_websocket_push_data_for_ohlcv(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["channel"].startswith(self.websocket_market_data_channel_ohlcv)

    def is_websocket_push_data_for_order(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["channel"] == self.websocket_account_channel_order

    def is_websocket_push_data_for_position(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["channel"] == self.websocket_account_channel_position

    def is_websocket_push_data_for_balance(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["channel"] == self.websocket_account_channel_balance

    def is_websocket_response_success(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return (payload_summary["event"] and payload_summary["event"] != "error") or payload_summary["code"] == "0"

    def is_websocket_response_for_create_order(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["op"] and payload_summary["op"] == "order"

    def is_websocket_response_for_cancel_order(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["op"] and payload_summary["op"] == "cancel-order"

    def is_websocket_response_for_subscribe(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["event"] and payload_summary["event"] == "subscribe"

    def is_websocket_response_for_login(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["event"] and payload_summary["event"] == "login"

    def convert_websocket_push_data_for_bbo(self, *, json_deserialized_payload):
        inst_id = json_deserialized_payload["arg"]["instId"]

        return [
            Bbo(
                api_method=ApiMethod.WEBSOCKET,
                symbol=inst_id,
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
                best_bid_price=x["bids"][0][0] if x["bids"] else None,
                best_bid_size=x["bids"][0][1] if x["bids"] else None,
                best_ask_price=x["asks"][0][0] if x["asks"] else None,
                best_ask_size=x["asks"][0][1] if x["asks"] else None,
            )
            for x in json_deserialized_payload["data"]
        ]

    def convert_websocket_push_data_for_trade(self, *, json_deserialized_payload):
        inst_id = json_deserialized_payload["arg"]["instId"]

        return [self.convert_dict_to_trade(input=x, api_method=ApiMethod.WEBSOCKET, symbol=inst_id) for x in json_deserialized_payload["data"]]

    def convert_websocket_push_data_for_ohlcv(self, *, json_deserialized_payload):
        inst_id = json_deserialized_payload["arg"]["instId"]

        return [self.convert_dict_to_ohlcv(input=x, api_method=ApiMethod.WEBSOCKET, symbol=inst_id) for x in json_deserialized_payload["data"]]

    def convert_websocket_push_data_for_order(self, *, json_deserialized_payload):
        return [self.convert_dict_to_order(input=x, api_method=ApiMethod.WEBSOCKET, symbol=x["instId"]) for x in json_deserialized_payload["data"]]

    def convert_websocket_push_data_for_fill(self, *, json_deserialized_payload):
        return [
            self.convert_dict_to_fill(input=x, api_method=ApiMethod.WEBSOCKET, symbol=x["instId"]) for x in json_deserialized_payload["data"] if x["tradeId"]
        ]

    def convert_websocket_push_data_for_position(self, *, json_deserialized_payload):
        return [self.convert_dict_to_position(input=x, api_method=ApiMethod.WEBSOCKET) for x in json_deserialized_payload["data"]]

    def convert_websocket_push_data_for_balance(self, *, json_deserialized_payload):
        return [self.convert_dict_to_balance(input=x, api_method=ApiMethod.WEBSOCKET) for x in json_deserialized_payload["data"][0]["balData"]]

    def convert_websocket_response_for_create_order(self, *, json_deserialized_payload, websocket_request):
        x = json_deserialized_payload["data"][0]

        return Order(
            api_method=ApiMethod.WEBSOCKET,
            symbol=websocket_request.json_payload["args"][0]["instId"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
            order_id=x["ordId"],
            client_order_id=websocket_request.json_payload["args"][0].get("clOrdId"),
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
            status=OrderStatus.CREATE_ACKNOWLEDGED,
        )

    def convert_websocket_response_for_cancel_order(self, *, json_deserialized_payload, websocket_request):
        x = json_deserialized_payload["data"][0]

        return Order(
            api_method=ApiMethod.WEBSOCKET,
            symbol=websocket_request.json_payload["args"][0]["instId"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
            order_id=websocket_request.json_payload["args"][0].get("ordId"),
            client_order_id=websocket_request.json_payload["args"][0].get("clOrdId"),
            status=OrderStatus.CANCEL_ACKNOWLEDGED,
        )

    async def handle_websocket_on_connected(self, *, websocket_connection):
        if websocket_connection.path == self.websocket_market_data_path:
            await self.websocket_market_data_subscribe_for_bbo_trade(websocket_connection=websocket_connection)

        elif websocket_connection.path == self.websocket_market_data_path_2:
            await self.websocket_market_data_subscribe_for_ohlcv(websocket_connection=websocket_connection)

        elif websocket_connection.path == self.websocket_account_path:
            await self.websocket_login(websocket_connection=websocket_connection)

    async def handle_websocket_push_data_for_order(self, *, websocket_message):

        if self.subscribe_order:
            await super().handle_websocket_push_data_for_order(websocket_message=websocket_message)

        if self.subscribe_fill:
            await super().handle_websocket_push_data_for_fill(websocket_message=websocket_message)

    async def handle_websocket_response_for_error(self, *, websocket_message):
        self.logger.warning("websocket_message", websocket_message)

        if self.is_websocket_response_for_create_order(websocket_message=websocket_message) or self.is_websocket_response_for_cancel_order(
            websocket_message=websocket_message
        ):
            await self.rest_account_fetch_order(
                symbol=websocket_message.websocket_request.json_payload["args"][0]["instId"],
                order_id=websocket_message.websocket_request.json_payload["args"][0].get("ordId"),
                client_order_id=websocket_message.websocket_request.json_payload["args"][0].get("clOrdId"),
            )

    def account_create_order_create_json_payload(self, *, order):
        if order.is_market:
            ord_type = "market"
        elif order.is_post_only:
            ord_type = "post_only"
        elif order.is_fok:
            ord_type = "fok"
        elif order.is_ioc:
            ord_type = "ioc"
        else:
            ord_type = "limit"

        json_payload = {
            "instId": order.symbol,
            "tdMode": str(order.margin_type) if order.margin_type else "cash",
            "clOrdId": order.client_order_id,
            "side": "buy" if order.is_buy else "sell",
            "ordType": ord_type,
            "sz": order.quantity,
            "tag": self.broker_id,
        }
        if order.price:
            json_payload["px"] = order.price
        if order.is_reduce_only:
            json_payload["reduceOnly"] = True
        if order.extra_params:
            json_payload.update(order.extra_params)
        return json_payload

    def account_cancel_order_create_json_payload(self, *, symbol, order_id=None, client_order_id=None):
        json_payload = {
            "instId": symbol,
        }
        if order_id:
            json_payload["ordId"] = order_id
        else:
            json_payload["clOrdId"] = client_order_id
        return json_payload

    def convert_dict_to_trade(self, *, input, api_method, symbol):
        return Trade(
            api_method=api_method,
            symbol=symbol,
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["ts"]),
            trade_id=input["tradeId"],
            price=input["px"],
            size=input["sz"],
            is_buyer_maker=input["side"] == "sell",
        )

    def convert_dict_to_ohlcv(self, *, input, api_method, symbol):
        return Ohlcv(
            api_method=api_method,
            symbol=symbol,
            start_unix_timestamp_seconds=int(input[0]) // 1000,
            open_price=input[1],
            high_price=input[2],
            low_price=input[3],
            close_price=input[4],
            volume=input[5],
            quote_volume=input[7],
        )

    def convert_dict_to_order(self, *, input, api_method, symbol):
        contract_size = 1

        if symbol in self.all_instrument_information and self.all_instrument_information[symbol].contract_size_as_decimal:
            contract_size = self.all_instrument_information[symbol].contract_size_as_decimal

        return Order(
            api_method=api_method,
            symbol=symbol,
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["uTime"]),
            order_id=input.get("ordId"),
            client_order_id=input.get("clOrdId"),
            is_buy=input["side"] == "buy",
            price=input["px"] or None,
            quantity=input["sz"],
            is_market=input["ordType"] == "market",
            is_post_only=input["ordType"] == "post_only",
            is_fok=input["ordType"] == "fok",
            is_ioc=input["ordType"] == "ioc",
            is_reduce_only=input["reduceOnly"] == "true",
            margin_type=MarginType[input["tdMode"].upper()] if input["tdMode"] != "cash" else None,
            cumulative_filled_quantity=input["accFillSz"] or None,
            cumulative_filled_quote_quantity="{0:f}".format(Decimal(input["avgPx"]) * Decimal(input["accFillSz"]) * contract_size) if input["avgPx"] else None,
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["cTime"]),
            status=self.order_status_mapping.get(input["state"]),
        )

    def convert_dict_to_fill(self, *, input, api_method, symbol):
        fill_fee = input.get("fillFee", input["fee"])
        fill_fee_ccy = input.get("fillFeeCcy", input["feeCcy"])
        is_rebate = not fill_fee.startswith("-")

        return Fill(
            api_method=api_method,
            symbol=symbol,
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["fillTime"]),
            order_id=input.get("ordId"),
            client_order_id=input.get("clOrdId"),
            trade_id=input["tradeId"],
            is_buy=input["side"] == "buy",
            price=input["fillPx"],
            quantity=input["fillSz"],
            fee_asset=fill_fee_ccy,
            fee_quantity=remove_leading_negative_sign_in_string(input=fill_fee),
            is_rebate=is_rebate,
        )

    def convert_dict_to_position(self, *, input, api_method):
        pos_side = input["posSide"]
        pos = input["pos"]
        symbol = input["instId"]
        is_long = None

        if pos_side == "long":
            is_long = True
        elif pos_side == "short":
            is_long = False
        else:
            if (
                self.instrument_type == OkxInstrumentType.FUTURES
                or self.instrument_type == OkxInstrumentType.SWAP
                or self.instrument_type == OkxInstrumentType.OPTION
            ):
                is_long = not pos.startswith("-")
            elif self.instrument_type == OkxInstrumentType.MARGIN:
                if symbol in self.all_instrument_information:
                    instrument_information_for_symbol = self.all_instrument_information[symbol]
                    pos_ccy = input["posCcy"]
                    if pos_ccy == instrument_information_for_symbol.base_asset:
                        is_long = True
                    elif pos_ccy == instrument_information_for_symbol.quote_asset:
                        is_long = False

        return Position(
            margin_type=MarginType[input["mgnMode"].upper()],
            api_method=api_method,
            symbol=symbol,
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["uTime"]),
            quantity=remove_leading_negative_sign_in_string(input=pos),
            is_long=is_long,
            entry_price=input["avgPx"],
            mark_price=input["markPx"],
            leverage=float(input["lever"]) if input["lever"] else None,
            initial_margin=float(input["imr"]) if input["imr"] else None,
            maintenance_margin=float(input["mmr"]) if input["mmr"] else None,
            unrealized_pnl=float(input["upl"]) if input["upl"] else None,
            liquidation_price=input["liqPx"],
        )

    def convert_dict_to_balance(self, *, input, api_method):
        return Balance(
            api_method=api_method,
            symbol=input["ccy"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["uTime"]),
            quantity=input["cashBal"],
        )

    def convert_ohlcv_interval_seconds_to_string(self, *, ohlcv_interval_seconds):
        if ohlcv_interval_seconds < 60:
            return f"{ohlcv_interval_seconds}s"
        elif ohlcv_interval_seconds < 3600:
            return f"{ohlcv_interval_seconds//60}m"
        elif ohlcv_interval_seconds < 86400:
            return f"{ohlcv_interval_seconds//3600}H"
        else:
            return f"{ohlcv_interval_seconds//86400}D"
