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


class BybitInstrumentType(StrEnum):
    SPOT = "spot"
    LINEAR = "linear"
    INVERSE = "inverse"
    OPTION = "option"


class Bybit(Exchange):

    def __init__(self, **kwargs) -> None:
        super().__init__(name="bybit", **kwargs)

        self.rest_market_data_base_url = "https://api.bybit.com"
        if self.is_paper_trading:
            self.rest_market_data_base_url = "https://api-testnet.bybit.com"
        self.rest_account_base_url = self.rest_market_data_base_url
        self.rest_market_data_fetch_all_instrument_information_path = "/v5/market/instruments-info"
        self.rest_market_data_fetch_all_instrument_information_limit = 1000
        self.rest_market_data_fetch_bbo_path = "/v5/market/tickers"
        self.rest_market_data_fetch_historical_trade_path = "/v5/market/recent-trade"
        self.rest_market_data_fetch_historical_trade_limit = 1000
        self.rest_market_data_fetch_historical_ohlcv_path = "/v5/market/kline"
        self.rest_market_data_fetch_historical_ohlcv_limit = 1000
        self.rest_account_create_order_path = "/v5/order/create"
        self.rest_account_cancel_order_path = "/v5/order/cancel"
        self.rest_account_fetch_order_path = "/v5/order/history"
        self.rest_account_fetch_open_order_path = "/v5/order/realtime"
        self.rest_account_fetch_open_order_limit = 50
        self.rest_account_fetch_position_path = "/v5/position/list"
        self.rest_account_fetch_position_limit = 200
        self.rest_account_fetch_balance_path = "/v5/account/wallet-balance"
        self.rest_account_fetch_historical_order_path = "/v5/order/history"
        self.rest_account_fetch_historical_order_limit = 50
        self.rest_account_fetch_historical_fill_path = "/v5/execution/list"
        self.rest_account_fetch_historical_fill_limit = 100

        self.websocket_market_data_base_url = "wss://stream.bybit.com"
        if self.is_paper_trading:
            self.websocket_market_data_base_url = "wss://stream-testnet.bybit.com"
        self.websocket_account_base_url = self.websocket_market_data_base_url
        self.websocket_market_data_path = f"/v5/public/{self.instrument_type}"
        self.websocket_market_data_channel_bbo = "orderbook.1."
        self.websocket_market_data_channel_trade = "publicTrade."
        self.websocket_market_data_channel_ohlcv = f"kline.{self.convert_ohlcv_interval_seconds_to_string(ohlcv_interval_seconds=self.ohlcv_interval_seconds)}."
        self.websocket_account_path = "/v5/private"
        self.websocket_account_channel_order = f"order.{self.instrument_type}"
        self.websocket_account_channel_fill = f"execution.{self.instrument_type}"
        self.websocket_account_channel_position = f"position.{self.instrument_type}"
        self.websocket_account_channel_balance = "wallet"
        self.websocket_account_trade_base_url = self.websocket_account_base_url
        self.websocket_account_trade_path = "/v5/trade"

        self.order_status_mapping = {
            "Cancelled": OrderStatus.CANCELED,
            "PartiallyFilledCanceled": OrderStatus.CANCELED,
            "New": OrderStatus.NEW,
            "PartiallyFilled": OrderStatus.PARTIALLY_FILLED,
            "Filled": OrderStatus.FILLED,
            "Untriggered": OrderStatus.UNTRIGGERED,
            "Triggered": OrderStatus.NEW,
            "Rejected": OrderStatus.REJECTED,
            "Deactivated": OrderStatus.CANCELED,
        }

        self.api_broker_id = "Vs000261"

        if self.instrument_type == BybitInstrumentType.SPOT:
            self.subscribe_position = False
            self.rest_account_fetch_position_period_seconds = None

        if self.instrument_type == BybitInstrumentType.SPOT:
            self.websocket_market_data_channel_symbols_limit = 10
        elif self.instrument_type == BybitInstrumentType.OPTION:
            self.websocket_market_data_channel_symbols_limit = 2000
        else:
            self.websocket_market_data_channel_symbols_limit = None

        self.api_receive_window_milliseconds = 5000

    def is_instrument_type_valid(self, *, instrument_type):
        return instrument_type in (
            BybitInstrumentType.SPOT,
            BybitInstrumentType.LINEAR,
            BybitInstrumentType.INVERSE,
            BybitInstrumentType.OPTION,
        )

    def convert_base_asset_quote_asset_to_symbol(self, *, base_asset, quote_asset):
        if self.instrument_type in (BybitInstrumentType.SPOT, BybitInstrumentType.LINEAR, BybitInstrumentType.INVERSE):
            return f"{base_asset}-{quote_asset}"
        else:
            return None

    def sign_request(self, *, rest_request, time_point):
        if rest_request.headers is None:
            rest_request.headers = {}

        headers = rest_request.headers
        headers["CONTENT-TYPE"] = "application/json"
        headers["X-BAPI-API-KEY"] = self.api_key
        headers["X-BAPI-TIMESTAMP"] = f"{int(convert_time_point_to_unix_timestamp_milliseconds(time_point=time_point))}"
        headers["X-BAPI-RECV-WINDOW"] = f"{self.api_receive_window_milliseconds}"
        payload = rest_request.query_string if rest_request.method == RestRequest.METHOD_GET else rest_request.payload

        signing_string = f"{headers['X-BAPI-TIMESTAMP']}{headers['X-BAPI-API-KEY']}{headers['X-BAPI-RECV-WINDOW']}{payload}"

        headers["X-BAPI-SIGN"] = hmac.new(
            bytes(self.api_secret, "utf-8"),
            bytes(signing_string, "utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()

        headers["X-Referer"] = self.api_broker_id

    def rest_market_data_fetch_all_instrument_information_create_rest_request_function(self):
        return self.rest_market_data_create_get_request_function(
            path=self.rest_market_data_fetch_all_instrument_information_path,
            query_params={"category": f"{self.instrument_type}", "limit": self.rest_market_data_fetch_all_instrument_information_limit},
        )

    def rest_market_data_fetch_bbo_create_rest_request_function(self):
        return self.rest_market_data_create_get_request_function(
            path=self.rest_market_data_fetch_bbo_path,
            query_params={"category": f"{self.instrument_type}"},
        )

    def rest_market_data_fetch_historical_trade_create_rest_request_function(self, *, symbol):
        return self.rest_market_data_create_get_request_function(
            path=self.rest_market_data_fetch_historical_trade_path, query_params={"symbol": symbol, "limit": self.rest_market_data_fetch_historical_trade_limit}
        )

    def rest_market_data_fetch_historical_ohlcv_create_rest_request_function(self, *, symbol):
        return self.rest_market_data_create_get_request_function(
            path=self.rest_market_data_fetch_historical_ohlcv_path,
            query_params={
                "symbol": symbol,
                "end": (
                    self.fetch_historical_ohlcv_end_unix_timestamp_seconds // self.ohlcv_interval_seconds * self.ohlcv_interval_seconds
                    - self.ohlcv_interval_seconds
                )
                * 1000,
                "interval": self.convert_ohlcv_interval_seconds_to_string(ohlcv_interval_seconds=self.ohlcv_interval_seconds),
                "limit": self.rest_market_data_fetch_historical_ohlcv_limit,
            },
        )

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
        query_params = {"category": f"{self.instrument_type}", "symbol": symbol}
        if order_id:
            query_params["orderId"] = order_id
        else:
            query_params["orderLinkId"] = client_order_id
        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_order_path, query_params=query_params)

    def rest_account_fetch_open_order_create_rest_request_function(self):
        query_params = {"category": f"{self.instrument_type}", "limit": self.rest_account_fetch_open_order_limit}
        if self.instrument_type == BybitInstrumentType.LINEAR:
            query_params["settleCoin"] = self.margin_asset

        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_open_order_path, query_params=query_params)

    def rest_account_fetch_position_create_rest_request_function(self):
        query_params = {"category": f"{self.instrument_type}"}
        if self.instrument_type == BybitInstrumentType.LINEAR:
            query_params["settleCoin"] = self.margin_asset
        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_position_path, query_params=query_params)

    def rest_account_fetch_balance_create_rest_request_function(self):
        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_balance_path, query_params={"accountType": "UNIFIED"})

    def rest_account_fetch_historical_order_create_rest_request_function(self, *, symbol):
        end_time = unix_timestamp_milliseconds_now() + 1
        start_time = max(end_time - 7 * 86400 * 1000, (self.fetch_historical_order_start_unix_timestamp_seconds or 0) * 1000)
        query_params = {
            "category": f"{self.instrument_type}",
            "symbol": symbol,
            "startTime": start_time,
            "endTime": end_time,
            "limit": self.rest_account_fetch_historical_order_limit,
        }

        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_historical_order_path, query_params=query_params)

    def rest_account_fetch_historical_fill_create_rest_request_function(self, *, symbol):
        return self.rest_account_create_get_request_function_with_signature(
            path=self.rest_account_fetch_historical_fill_path,
            query_params={"category": f"{self.instrument_type}", "symbol": symbol, "limit": self.rest_account_fetch_historical_fill_limit},
        )

    def is_rest_response_success(self, *, rest_response):
        return (
            super().is_rest_response_success(rest_response=rest_response)
            and rest_response.json_deserialized_payload
            and rest_response.json_deserialized_payload["retCode"] == 0
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
        return rest_response.rest_request.path == self.rest_account_create_order_path

    def is_rest_response_for_cancel_order(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_cancel_order_path

    def is_rest_response_for_fetch_order(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_order_path and (
            " orderId" in rest_response.rest_request.query_params or " orderLinkId" in rest_response.rest_request.query_params
        )

    def is_rest_response_for_fetch_open_order(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_open_order_path

    def is_rest_response_for_fetch_position(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_position_path

    def is_rest_response_for_fetch_balance(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_balance_path

    def is_rest_response_for_historical_order(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_historical_order_path

    def is_rest_response_for_historical_fill(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_historical_fill_path

    def convert_rest_response_for_all_instrument_information(self, *, json_deserialized_payload, rest_request):
        return [
            InstrumentInformation(
                api_method=ApiMethod.REST,
                symbol=x["symbol"],
                base_asset=x["baseCoin"],
                quote_asset=x["quoteCoin"],
                order_price_increment=normalize_decimal_string(input=x["priceFilter"]["tickSize"]),
                order_quantity_increment=(
                    normalize_decimal_string(input=x["lotSizeFilter"]["basePrecision"])
                    if self.instrument_type == BybitInstrumentType.SPOT
                    else normalize_decimal_string(input=x["lotSizeFilter"]["qtyStep"])
                ),
                order_quantity_min=normalize_decimal_string(input=x["lotSizeFilter"]["minOrderQty"]),
                order_quote_quantity_min=(
                    normalize_decimal_string(input=x["lotSizeFilter"]["minOrderAmt"])
                    if self.instrument_type == BybitInstrumentType.SPOT
                    else (
                        normalize_decimal_string(input=x["lotSizeFilter"]["minNotionalValue"])
                        if self.instrument_type in (BybitInstrumentType.LINEAR, BybitInstrumentType.INVERSE)
                        else None
                    )
                ),
                order_quantity_max=(normalize_decimal_string(input=x["lotSizeFilter"]["maxOrderQty"])),
                order_quote_quantity_max=(
                    normalize_decimal_string(input=x["lotSizeFilter"]["maxOrderAmt"]) if self.instrument_type == BybitInstrumentType.SPOT else None
                ),
                margin_asset=x["settleCoin"] if self.instrument_type != BybitInstrumentType.SPOT else None,
                expiry_time=int(x["deliveryTime"]) // 1000 if self.instrument_type != BybitInstrumentType.SPOT else None,
                is_open_for_trade=x["status"] in ("Trading", "PreLaunch"),
            )
            for x in json_deserialized_payload["result"]["list"]
        ]

    def convert_rest_response_for_bbo(self, *, json_deserialized_payload, rest_request):
        exchange_update_time_point = convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["time"])
        return [
            Bbo(
                api_method=ApiMethod.REST,
                symbol=symbol,
                exchange_update_time_point=exchange_update_time_point,
                best_bid_price=x.get("bid1Price"),
                best_bid_size=x.get("bid1Size"),
                best_ask_price=x.get("ask1Price"),
                best_ask_size=x.get("ask1Size"),
            )
            for x in json_deserialized_payload["result"]["list"]
            if (symbol := x["symbol"]) in self.symbols
        ]

    def convert_rest_response_for_historical_trade(self, *, json_deserialized_payload, rest_request):
        return [
            Trade(
                api_method=ApiMethod.REST,
                symbol=x["symbol"],
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=int(x["time"])),
                trade_id=x["execId"],
                is_trade_id_monotonic_increase=False,
                price=x["price"],
                size=x["size"],
                is_buyer_maker=x["side"] == "Sell",
            )
            for x in json_deserialized_payload["result"]["list"]
        ]

    def convert_rest_response_for_historical_trade_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        pass

    def convert_rest_response_for_historical_ohlcv(self, *, json_deserialized_payload, rest_request):
        symbol = rest_request.query_params["symbol"]

        return [
            Ohlcv(
                api_method=ApiMethod.REST,
                symbol=symbol,
                start_unix_timestamp_seconds=int(x[0]) // 1000,
                open_price=x[1],
                high_price=x[2],
                low_price=x[3],
                close_price=x[4],
                volume=x[5],
                quote_volume=x[6],
            )
            for x in json_deserialized_payload["result"]["list"]
        ]

    def convert_rest_response_for_historical_ohlcv_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        data = json_deserialized_payload["result"]["list"]

        if data:
            head = data[0]
            head_ts = int(head[0])
            tail = data[-1]
            tail_ts = int(tail[0])

            if head_ts < tail_ts:
                end = head_ts - self.ohlcv_interval_seconds * 1000
            else:
                end = tail_ts - self.ohlcv_interval_seconds * 1000

            if self.fetch_historical_ohlcv_start_unix_timestamp_seconds is None or end // 1000 >= self.fetch_historical_ohlcv_start_unix_timestamp_seconds:
                return self.rest_market_data_create_get_request_function(
                    path=self.rest_market_data_fetch_historical_ohlcv_path,
                    query_params={
                        "symbol": rest_request.query_params["symbol"],
                        "end": end,
                        "interval": self.convert_ohlcv_interval_seconds_to_string(ohlcv_interval_seconds=self.ohlcv_interval_seconds),
                        "limit": self.rest_market_data_fetch_historical_ohlcv_limit,
                    },
                )

    def convert_rest_response_for_create_order(self, *, json_deserialized_payload, rest_request):
        x = json_deserialized_payload["result"]

        return Order(
            api_method=ApiMethod.REST,
            symbol=rest_request.json_payload["symbol"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["time"]),
            order_id=x["orderId"],
            client_order_id=x["orderLinkId"],
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["time"]),
            status=OrderStatus.CREATE_ACKNOWLEDGED,
        )

    def convert_rest_response_for_cancel_order(self, *, json_deserialized_payload, rest_request):
        x = json_deserialized_payload["result"]

        return Order(
            api_method=ApiMethod.REST,
            symbol=rest_request.json_payload["symbol"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["time"]),
            order_id=x["orderId"],
            client_order_id=x["orderLinkId"],
            status=OrderStatus.CANCEL_ACKNOWLEDGED,
        )

    def convert_rest_response_for_fetch_order(self, *, json_deserialized_payload, rest_request):
        if json_deserialized_payload["result"]["list"]:
            x = json_deserialized_payload["result"]["list"][0]

            return self.convert_dict_to_order(input=x, api_method=ApiMethod.REST, symbol=x["symbol"])
        else:
            return Order(
                api_method=ApiMethod.REST,
                symbol=rest_request.query_params["symbol"],
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["time"]),
                order_id=rest_request.query_params.get("orderId"),
                client_order_id=rest_request.query_params.get("orderLinkId"),
                status=OrderStatus.REJECTED,
            )

    def convert_rest_response_for_fetch_open_order(self, *, json_deserialized_payload, rest_request):
        return [self.convert_dict_to_order(input=x, api_method=ApiMethod.REST, symbol=x["symbol"]) for x in json_deserialized_payload["result"]["list"]]

    def convert_rest_response_for_fetch_open_order_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        cursor = json_deserialized_payload["result"].get("nextPagerCursor")

        if cursor:
            query_params = {"category": f"{self.instrument_type}", "cursor": cursor, "limit": self.rest_account_fetch_open_order_limit}
            if self.instrument_type == BybitInstrumentType.LINEAR:
                query_params["settleCoin"] = self.margin_asset

            return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_open_order_path, query_params=query_params)

    def convert_rest_response_for_fetch_position(self, *, json_deserialized_payload, rest_request):
        return [self.convert_dict_to_position(input=x, api_method=ApiMethod.REST) for x in json_deserialized_payload["result"]["list"]]

    def convert_rest_response_for_fetch_balance(self, *, json_deserialized_payload, rest_request):
        return [self.convert_dict_to_balance(input=x, api_method=ApiMethod.REST) for x in json_deserialized_payload["result"]["list"][0]["coin"]]

    def convert_rest_response_for_historical_order(self, *, json_deserialized_payload, rest_request):
        symbol = rest_request.query_params["symbol"]

        return [self.convert_dict_to_order(input=x, api_method=ApiMethod.REST, symbol=symbol) for x in json_deserialized_payload["result"]["list"]]

    def convert_rest_response_for_historical_order_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        symbol = rest_request.query_params["symbol"]
        data = json_deserialized_payload["result"]["list"]
        cursor = json_deserialized_payload["result"].get("nextPagerCursor")
        start_time = rest_request.query_params.get("startTime")
        end_time = rest_request.query_params.get("endTime")

        if data and cursor:
            query_params = {
                "category": f"{self.instrument_type}",
                "symbol": symbol,
                "cursor": cursor,
                "limit": self.rest_account_fetch_historical_order_limit,
            }
            if start_time:
                query_params["startTime"] = start_time
            if end_time:
                query_params["endTime"] = end_time

            return self.rest_account_create_get_request_function_with_signature(path=rest_request.path, query_params=query_params)
        elif start_time and (
            self.fetch_historical_order_start_unix_timestamp_seconds is None or start_time > self.fetch_historical_order_start_unix_timestamp_seconds * 1000
        ):
            end_time = start_time
            start_time = max(end_time - 7 * 86400 * 1000, (self.fetch_historical_order_start_unix_timestamp_seconds or 0) * 1000)

            return self.rest_account_create_get_request_function_with_signature(
                path=rest_request.path,
                query_params={
                    "category": f"{self.instrument_type}",
                    "symbol": symbol,
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": self.rest_account_fetch_historical_order_limit,
                },
            )

    def convert_rest_response_for_historical_fill(self, *, json_deserialized_payload, rest_request):
        symbol = rest_request.query_params["symbol"]

        return [self.convert_dict_to_fill(input=x, api_method=ApiMethod.REST, symbol=symbol) for x in json_deserialized_payload["result"]["list"]]

    def convert_rest_response_for_historical_fill_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        symbol = rest_request.query_params["symbol"]
        data = json_deserialized_payload["result"]["list"]
        cursor = json_deserialized_payload["result"].get("nextPagerCursor")
        start_time = rest_request.query_params.get("startTime")
        end_time = rest_request.query_params.get("endTime")

        if data and cursor:
            query_params = {
                "category": f"{self.instrument_type}",
                "symbol": symbol,
                "cursor": cursor,
                "limit": self.rest_account_fetch_historical_fill_limit,
            }
            if start_time:
                query_params["startTime"] = start_time
            if end_time:
                query_params["endTime"] = end_time

            return self.rest_account_create_get_request_function_with_signature(path=rest_request.path, query_params=query_params)
        elif start_time and (
            self.fetch_historical_fill_start_unix_timestamp_seconds is None or start_time > self.fetch_historical_fill_start_unix_timestamp_seconds * 1000
        ):
            end_time = start_time
            start_time = max(end_time - 7 * 86400 * 1000, (self.fetch_historical_fill_start_unix_timestamp_seconds or 0) * 1000)

            return self.rest_account_create_get_request_function_with_signature(
                path=rest_request.path,
                query_params={
                    "category": f"{self.instrument_type}",
                    "symbol": symbol,
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": self.rest_account_fetch_historical_fill_limit,
                },
            )

    async def handle_rest_response_for_error(self, *, rest_response):
        self.logger.warning("rest_response", rest_response)

        if self.is_rest_response_for_create_order(rest_response=rest_response) or self.is_rest_response_for_cancel_order(rest_response=rest_response):

            async def start_rest_account_fetch_order():
                try:
                    await self.rest_account_fetch_order(
                        symbol=rest_response.rest_request.json_payload["symbol"],
                        order_id=rest_response.rest_request.json_payload.get("orderId"),
                        client_order_id=rest_response.rest_request.json_payload.get("orderLinkId"),
                    )
                except Exception as exception:
                    self.logger.error(exception)

            self.create_task(coro=start_rest_account_fetch_order())

        elif self.is_rest_response_for_fetch_order(rest_response=rest_response):
            if (
                rest_response.status_code == 200
                and rest_response.json_deserialized_payload
                and rest_response.json_deserialized_payload.get("code") in (110001,)
            ):
                now_time_point = time_point_now()
                self.replace_order(
                    symbol=rest_response.rest_request.query_params["symbol"],
                    order_id=rest_response.rest_request.query_params.get("orderId"),
                    client_order_id=rest_response.rest_request.query_params.get("orderLinkId"),
                    exchange_update_time_point=now_time_point,
                    local_update_time_point=now_time_point,
                    status=OrderStatus.REJECTED,
                )

    def websocket_connection_ping_on_application_level_create_websocket_request(self):
        id = self.generate_next_websocket_request_id()
        payload = self.json_serialize({"req_id": id, "op": "ping"})
        self.logger.trace("send application level ping")
        return self.websocket_create_request(payload=payload)

    def websocket_login_create_websocket_request(self, *, time_point):
        id = self.generate_next_websocket_request_id()
        expires = int(convert_time_point_to_unix_timestamp_milliseconds(time_point=time_point) + self.api_receive_window_milliseconds)
        signature = hmac.new(bytes(self.api_secret, "utf-8"), bytes(f"GET/realtime{expires}", "utf-8"), digestmod=hashlib.sha256).hexdigest()

        payload = self.json_serialize(
            {
                "req_id": id,
                "op": "auth",
                "args": [self.api_key, expires, signature],
            }
        )

        return self.websocket_create_request(payload=payload)

    def websocket_market_data_update_subscribe_create_websocket_request(self, *, symbols, is_subscribe):
        if self.subscribe_bbo or self.subscribe_trade or self.subscribe_ohlcv:
            args = []

            for symbol in symbols:
                if self.subscribe_bbo:
                    args.append(f"{self.websocket_market_data_channel_bbo}{symbol}")
                if self.subscribe_trade:
                    args.append(f"{self.websocket_market_data_channel_trade}{symbol}")
                if self.subscribe_ohlcv:
                    args.append(f"{self.websocket_market_data_channel_ohlcv}{symbol}")

            id = self.generate_next_websocket_request_id()
            payload = self.json_serialize({"req_id": id, "op": "subscribe", "args": args})
            return self.websocket_create_request(payload=payload)
        else:
            return None

    def websocket_account_update_subscribe_create_websocket_request(self, *, is_subscribe):
        args = []

        if self.subscribe_order:
            args.append(f"{self.websocket_account_channel_order}")

        if self.subscribe_fill:
            args.append(f"{self.websocket_account_channel_fill}")

        if self.subscribe_position:
            args.append(f"{self.websocket_account_channel_position}")

        if self.subscribe_balance:
            args.append(f"{self.websocket_account_channel_balance}")

        id = self.generate_next_websocket_request_id()
        payload = self.json_serialize({"req_id": id, "op": "subscribe", "args": args})
        return self.websocket_create_request(payload=payload)

    def websocket_account_create_order_create_websocket_request(self, *, order):
        id = self.generate_next_websocket_request_id()
        header = {}
        now_time_point = time_point_now()
        header["X-BAPI-TIMESTAMP"] = f"{int(convert_time_point_to_unix_timestamp_milliseconds(time_point=now_time_point))}"
        header["X-BAPI-RECV-WINDOW"] = f"{self.api_receive_window_milliseconds}"
        header["Referer"] = self.api_broker_id
        arg = self.account_create_order_create_json_payload(order=order)
        return WebsocketRequest(id=id, json_payload={"reqId": id, "header": header, "op": "order.create", "args": [arg]}, json_serialize=self.json_serialize)

    def websocket_account_cancel_order_create_websocket_request(self, *, symbol, order_id=None, client_order_id=None):
        id = self.generate_next_websocket_request_id()
        header = {}
        now_time_point = time_point_now()
        header["X-BAPI-TIMESTAMP"] = f"{int(convert_time_point_to_unix_timestamp_milliseconds(time_point=now_time_point))}"
        header["X-BAPI-RECV-WINDOW"] = f"{self.api_receive_window_milliseconds}"
        header["Referer"] = self.api_broker_id
        arg = self.account_cancel_order_create_json_payload(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
        return WebsocketRequest(id=id, json_payload={"reqId": id, "header": header, "op": "order.cancel", "args": [arg]}, json_serialize=self.json_serialize)

    def websocket_on_message_extract_data(self, *, websocket_connection, websocket_message):
        json_deserialized_payload = websocket_message.json_deserialized_payload

        websocket_message.payload_summary = {
            "success": json_deserialized_payload.get("success"),
            "op": json_deserialized_payload.get("op"),
            "topic": json_deserialized_payload.get("topic"),
            "retCode": json_deserialized_payload.get("retCode"),
        }

        id = (
            json_deserialized_payload.get("reqId")
            if websocket_connection.path == self.websocket_account_trade_path
            else json_deserialized_payload.get("req_id")
        )
        websocket_message.websocket_request_id = str(id) if id is not None else None

        if websocket_message.websocket_request_id:
            websocket_message.websocket_request = self.websocket_requests.get(websocket_message.websocket_request_id)

        return websocket_message

    def is_websocket_push_data(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["topic"] is not None

    def is_websocket_push_data_for_bbo(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["topic"].startswith(self.websocket_market_data_channel_bbo)

    def is_websocket_push_data_for_trade(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["topic"].startswith(self.websocket_market_data_channel_trade)

    def is_websocket_push_data_for_ohlcv(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["topic"].startswith(self.websocket_market_data_channel_ohlcv)

    def is_websocket_push_data_for_order(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["topic"] == self.websocket_account_channel_order

    def is_websocket_push_data_for_position(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["topic"] == self.websocket_account_channel_position

    def is_websocket_push_data_for_balance(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["topic"] == self.websocket_account_channel_balance

    def is_websocket_response_success(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["success"] or (payload_summary["retCode"] is not None and payload_summary["retCode"] == 0)

    def is_websocket_response_for_create_order(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["op"] and payload_summary["op"] == "order.create"

    def is_websocket_response_for_cancel_order(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["op"] and payload_summary["op"] == "order.cancel"

    def is_websocket_response_for_subscribe(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["op"] and payload_summary["op"] == "subscribe"

    def is_websocket_response_for_login(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["op"] and payload_summary["op"] == "auth"

    def is_websocket_response_for_ping_on_application_level(self, *, websocket_message):
        payload_summary = websocket_message.payload_summary
        return payload_summary["op"] and payload_summary["op"] == "ping"

    def convert_websocket_push_data_for_bbo(self, *, json_deserialized_payload):
        topic = json_deserialized_payload["topic"]
        symbol = topic[topic.rfind(".") + 1 :]
        data = json_deserialized_payload["data"]

        return [
            Bbo(
                api_method=ApiMethod.WEBSOCKET,
                symbol=symbol,
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["cts"]),
                best_bid_price=data["b"][0][0] if data.get("b") else None,
                best_bid_size=data["b"][0][1] if data.get("b") else None,
                best_ask_price=data["a"][0][0] if data.get("a") else None,
                best_ask_size=data["a"][0][1] if data.get("a") else None,
            )
        ]

    def convert_websocket_push_data_for_trade(self, *, json_deserialized_payload):
        topic = json_deserialized_payload["topic"]
        symbol = topic[topic.find(".") + 1 :]

        return [
            Trade(
                api_method=ApiMethod.WEBSOCKET,
                symbol=symbol,
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["T"]),
                trade_id=x["i"],
                is_trade_id_monotonic_increase=False,
                price=x["p"],
                size=x["v"],
                is_buyer_maker=x["S"] == "Sell",
            )
            for x in json_deserialized_payload["data"]
        ]

    def convert_websocket_push_data_for_ohlcv(self, *, json_deserialized_payload):
        topic = json_deserialized_payload["topic"]
        symbol = topic[topic.find(".") + 1 :]

        return [
            Ohlcv(
                api_method=ApiMethod.WEBSOCKET,
                symbol=symbol,
                start_unix_timestamp_seconds=int(x["start"]) // 1000,
                open_price=x["open"],
                high_price=x["high"],
                low_price=x["low"],
                close_price=x["close"],
                volume=x["volume"],
                quote_volume=x["turnover"],
            )
            for x in json_deserialized_payload["data"]
        ]

    def convert_websocket_push_data_for_order(self, *, json_deserialized_payload):
        return [self.convert_dict_to_order(input=x, api_method=ApiMethod.WEBSOCKET, symbol=x["symbol"]) for x in json_deserialized_payload["data"]]

    def convert_websocket_push_data_for_fill(self, *, json_deserialized_payload):
        return [self.convert_dict_to_fill(input=x, api_method=ApiMethod.WEBSOCKET, symbol=x["symbol"]) for x in json_deserialized_payload["data"]]

    def convert_websocket_push_data_for_position(self, *, json_deserialized_payload):
        return [self.convert_dict_to_position(input=x, api_method=ApiMethod.WEBSOCKET) for x in json_deserialized_payload["data"]]

    def convert_websocket_push_data_for_balance(self, *, json_deserialized_payload):
        return [self.convert_dict_to_balance(input=x, api_method=ApiMethod.WEBSOCKET) for x in json_deserialized_payload["data"][0]["coin"]]

    def convert_websocket_response_for_create_order(self, *, json_deserialized_payload, websocket_request):
        x = json_deserialized_payload["data"]

        return Order(
            api_method=ApiMethod.WEBSOCKET,
            symbol=websocket_request.json_payload["args"][0]["symbol"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(
                unix_timestamp_milliseconds=json_deserialized_payload["header"]["Timenow"]
            ),
            order_id=x["orderId"],
            client_order_id=websocket_request.json_payload["args"][0].get("orderLinkId"),
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(
                unix_timestamp_milliseconds=json_deserialized_payload["header"]["Timenow"]
            ),
            status=OrderStatus.CREATE_ACKNOWLEDGED,
        )

    def convert_websocket_response_for_cancel_order(self, *, json_deserialized_payload, websocket_request):
        x = json_deserialized_payload["data"]

        return Order(
            api_method=ApiMethod.WEBSOCKET,
            symbol=websocket_request.json_payload["args"][0]["symbol"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(
                unix_timestamp_milliseconds=json_deserialized_payload["header"]["Timenow"]
            ),
            order_id=x["orderId"],
            client_order_id=websocket_request.json_payload["args"][0].get("orderLinkId"),
            status=OrderStatus.CANCEL_ACKNOWLEDGED,
        )

    async def handle_websocket_response_for_error(self, *, websocket_message):
        self.logger.warning("websocket_message", websocket_message)

        if self.is_websocket_response_for_create_order(websocket_message=websocket_message) or self.is_websocket_response_for_cancel_order(
            websocket_message=websocket_message
        ):

            async def start_rest_account_fetch_order():
                try:
                    await self.rest_account_fetch_order(
                        symbol=websocket_message.websocket_request.json_payload["args"][0]["symbol"],
                        order_id=websocket_message.websocket_request.json_payload["args"][0].get("orderId"),
                        client_order_id=websocket_message.websocket_request.json_payload["args"][0].get("orderLinkId"),
                    )
                except Exception as exception:
                    self.logger.error(exception)

            self.create_task(coro=start_rest_account_fetch_order())

    def account_create_order_create_json_payload(self, *, order):
        if order.is_post_only:
            time_in_force = "PostOnly"
        elif order.is_fok:
            time_in_force = "FOK"
        elif order.is_ioc:
            time_in_force = "IOC"
        else:
            time_in_force = "GTC"

        json_payload = {
            "category": self.instrument_type.value,
            "symbol": order.symbol,
            "orderLinkId": order.client_order_id,
            "side": "Buy" if order.is_buy else "Sell",
            "orderType": "Market" if order.is_market else "Limit",
            "qty": order.quantity,
            "timeInForce": time_in_force,
        }
        if order.price:
            json_payload["price"] = order.price
        if order.is_reduce_only:
            json_payload["reduceOnly"] = True
        if order.extra_params:
            json_payload.update(order.extra_params)

        return json_payload

    def account_cancel_order_create_json_payload(self, *, symbol, order_id=None, client_order_id=None):
        json_payload = {
            "category": self.instrument_type.value,
            "symbol": symbol,
        }
        if order_id:
            json_payload["orderId"] = order_id
        else:
            json_payload["orderLinkId"] = client_order_id
        return json_payload

    def convert_dict_to_order(self, *, input, api_method, symbol):
        if symbol in self.all_instrument_information and self.all_instrument_information[symbol].contract_size_as_decimal:
            self.all_instrument_information[symbol].contract_size_as_decimal

        return Order(
            api_method=api_method,
            symbol=symbol,
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["updatedTime"]),
            order_id=input.get("orderId"),
            client_order_id=input.get("orderLinkId"),
            is_buy=input["side"] == "Buy",
            price=input["price"] or None,
            quantity=input["qty"],
            is_market=input["orderType"] == "Market",
            is_post_only=input["timeInForce"] == "PostOnly",
            is_fok=input["timeInForce"] == "FOK",
            is_ioc=input["timeInForce"] == "IOC",
            is_reduce_only=input["reduceOnly"],
            cumulative_filled_quantity=input["cumExecQty"] or None,
            cumulative_filled_quote_quantity=input["cumExecValue"] or None,
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["createdTime"]),
            status=self.order_status_mapping[input["orderStatus"]],
        )

    def convert_dict_to_fill(self, *, input, api_method, symbol):
        exec_fee = input.get("execFee")

        return Fill(
            api_method=api_method,
            symbol=symbol,
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["execTime"]),
            order_id=input.get("orderId"),
            client_order_id=input.get("orderLinkId"),
            trade_id=input["execId"],
            is_trade_id_monotonic_increase=False,
            is_buy=input["side"] == "Buy",
            price=input["execPrice"],
            quantity=input["execQty"],
            is_maker=input["isMaker"],
            fee_asset=input.get("feeCurrency"),
            fee_quantity=remove_leading_negative_sign_in_string(input=exec_fee) if exec_fee else None,
            is_fee_rebate=exec_fee.startswith("-") if exec_fee else None,
        )

    def convert_dict_to_position(self, *, input, api_method):
        return Position(
            api_method=api_method,
            symbol=input["symbol"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["updatedTime"]),
            quantity=input["size"],
            is_long=input["side"] == "Buy" if input["side"] else None,
            entry_price=input["avgPrice"] if api_method == ApiMethod.REST else input["entryPrice"],
            mark_price=input["markPrice"],
            leverage=input["leverage"],
            initial_margin=input["positionIM"],
            maintenance_margin=input["positionMM"],
            unrealized_pnl=input["unrealisedPnl"],
            liquidation_price=input["liqPrice"],
        )

    def convert_dict_to_balance(self, *, input, api_method):
        return Balance(
            api_method=api_method,
            symbol=input["coin"],
            quantity=input["walletBalance"],
        )

    def convert_ohlcv_interval_seconds_to_string(self, *, ohlcv_interval_seconds):
        if ohlcv_interval_seconds < 86400:
            return f"{ohlcv_interval_seconds//60}"
        elif ohlcv_interval_seconds < 604800:
            return "D"
        else:
            return "W"
