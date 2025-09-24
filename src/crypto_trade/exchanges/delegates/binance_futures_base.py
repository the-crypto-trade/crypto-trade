import hashlib
import hmac
import base64
import asyncio
from cryptography.hazmat.primitives.serialization import load_pem_private_key
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
    Logger,
    LoggerApi,
    LogLevel,
    RestRequest,
    RestResponse,
    WebsocketConnection,
    WebsocketMessage,
    WebsocketRequest,
    convert_set_to_subsets,
    convert_time_point_delta_to_seconds,
    create_url,
    create_url_with_query_params,
    time_point_now,
    time_point_subtract,
    unix_timestamp_seconds_now,
)
from crypto_trade.utility import (
    RestRequest,
    RestResponse,
    WebsocketRequest,
    convert_time_point_to_unix_timestamp_milliseconds,
    convert_unix_timestamp_milliseconds_to_time_point,
    normalize_decimal_string,
    remove_leading_negative_sign_in_string,
    time_point_now,
    unix_timestamp_milliseconds_now,
)
from crypto_trade.exchanges.delegates.binance_base import BinanceBase


class BinanceFuturesBase(BinanceBase):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.rest_account_start_user_data_stream_path = None
        self.rest_account_keepalive_user_data_stream_path = None
        self.websocket_account_system_event_listen_key_expired = 'listenKeyExpired'

        self.order_status_mapping = {
            "NEW": OrderStatus.NEW,
            "PARTIALLY_FILLED": OrderStatus.PARTIALLY_FILLED,
            "FILLED": OrderStatus.FILLED,
            "CANCELED": OrderStatus.CANCELED,
            "REJECTED": OrderStatus.REJECTED,
            "EXPIRED": OrderStatus.EXPIRED,
            "EXPIRED_IN_MATCH": OrderStatus.EXPIRED,
        }

        self.api_link_id = "QzcVS63u"

        self.websocket_market_data_channel_symbols_limit = 1024

        self.api_receive_window_milliseconds = 5000




    def convert_base_asset_quote_asset_to_symbol(self, *, base_asset, quote_asset):
        return f"{base_asset}{quote_asset}"

    def sign_request(self, *, rest_request, time_point):
        if rest_request.headers is None:
            rest_request.headers = {}

        headers = rest_request.headers
        headers["X-MBX-APIKEY"] = self.api_key

        query_string = f'{rest_request.query_string}&' if rest_request.query_string else ''
        query_string += f"timestamp={int(convert_time_point_to_unix_timestamp_milliseconds(time_point=time_point))}"
        query_string += f"&recvWindow={self.api_receive_window_milliseconds}"

        signature = hmac.new(
            bytes(self.api_secret, "utf-8"),
            bytes(query_string, "utf-8"),
            digestmod=hashlib.sha256,
        ).hexdigest()

        query_string += f"&signature={signature}"

        rest_request.query_string = query_string

    def rest_market_data_fetch_all_instrument_information_create_rest_request_function(self):
        return self.rest_market_data_create_get_request_function(
            path=self.rest_market_data_fetch_all_instrument_information_path,
        )

    def rest_market_data_fetch_bbo_create_rest_request_function(self):
        return self.rest_market_data_create_get_request_function(
            path=self.rest_market_data_fetch_bbo_path,
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
                "endTime": (
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
            query_params=self.account_create_order_create_params(order=order),
        )

    def rest_account_cancel_order_create_rest_request_function(self, *, symbol, order_id=None, client_order_id=None):
        return self.rest_account_create_delete_request_function_with_signature(
            path=self.rest_account_cancel_order_path,
            query_params=self.account_cancel_order_create_params(symbol=symbol, order_id=order_id, client_order_id=client_order_id),
        )

    def rest_account_fetch_order_create_rest_request_function(self, *, symbol, order_id=None, client_order_id=None):
        query_params = { "symbol": symbol}
        if order_id:
            query_params["orderId"] = order_id
        else:
            query_params["origClientOrderId"] = client_order_id
        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_order_path, query_params=query_params)

    def rest_account_fetch_open_order_create_rest_request_function(self):
        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_open_order_path)

    def rest_account_fetch_position_create_rest_request_function(self):
        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_position_path)

    def rest_account_fetch_balance_create_rest_request_function(self):
        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_balance_path)

    def rest_account_fetch_historical_order_create_rest_request_function(self, *, symbol):
        end_time = unix_timestamp_milliseconds_now() + 1
        start_time = max(end_time - 7 * 86400 * 1000, (self.fetch_historical_order_start_unix_timestamp_seconds or 0) * 1000)
        query_params = {
            "symbol": symbol,
            "startTime": start_time,
            "endTime": end_time,
            "limit": self.rest_account_fetch_historical_order_limit,
        }

        return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_historical_order_path, query_params=query_params)

    def rest_account_fetch_historical_fill_create_rest_request_function(self, *, symbol):
        return self.rest_account_create_get_request_function_with_signature(
            path=self.rest_account_fetch_historical_fill_path,
            query_params={ "symbol": symbol, "limit": self.rest_account_fetch_historical_fill_limit},
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
        return rest_response.rest_request.path == self.rest_account_cancel_order_path and rest_response.rest_request.method == RestRequest.METHOD_DELETE

    def is_rest_response_for_fetch_order(self, *, rest_response):
        return rest_response.rest_request.path == self.rest_account_fetch_order_path and rest_response.rest_request.method == RestRequest.METHOD_GET

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
        result = []
        for x in json_deserialized_payload["symbols"]:
            filters = {y['filterType']:y for y in x['filters']}
            result.append (
                InstrumentInformation(
                    api_method=ApiMethod.REST,
                    symbol=x["symbol"],
                    base_asset=x["baseAsset"],
                    quote_asset=x["quoteAsset"],
                    order_price_increment=normalize_decimal_string(input=filters["PRICE_FILTER"]["tickSize"]),
                    order_quantity_increment=(
                        normalize_decimal_string(input=filters["LOT_SIZE"]["stepSize"])
                    ),
                    order_quantity_min=normalize_decimal_string(input=filters["LOT_SIZE"]["minQty"]),
                    order_quote_quantity_min=(
                        normalize_decimal_string(input=filters["MIN_NOTIONAL"]["notional"])
                    ),
                    order_quantity_max=(normalize_decimal_string(input=filters["LOT_SIZE"]["maxQty"])),
                    margin_asset=x["marginAsset"],
                    contract_size=str(x["contractSize"]) if x.get('contractSize') else None,
                    expiry_time=int(x["deliveryDate"]) // 1000,
                    is_open_for_trade=x["status"] == "TRADING",
                )
            )
        return result

    def convert_rest_response_for_bbo(self, *, json_deserialized_payload, rest_request):
        return [
            Bbo(
                api_method=ApiMethod.REST,
                symbol=symbol,
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["time"]),
                best_bid_price=x.get("bidPrice"),
                best_bid_size=x.get("bidQty"),
                best_ask_price=x.get("askPrice"),
                best_ask_size=x.get("askQty"),
            )
            for x in json_deserialized_payload
            if (symbol := x["symbol"]) in self.symbols
        ]

    def convert_rest_response_for_historical_trade(self, *, json_deserialized_payload, rest_request):
        return [
            Trade(
                api_method=ApiMethod.REST,
                symbol=x["symbol"],
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=int(x["time"])),
                trade_id=str(x["id"]),
                price=x["price"],
                size=x["qty"],
                is_buyer_maker=x["isBuyerMaker"],
            )
            for x in json_deserialized_payload
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
                base_volume=x[7],
            )
            for x in json_deserialized_payload
        ]

    def convert_rest_response_for_historical_ohlcv_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        data = json_deserialized_payload

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
                        "endTime": end,
                        "interval": self.convert_ohlcv_interval_seconds_to_string(ohlcv_interval_seconds=self.ohlcv_interval_seconds),
                        "limit": self.rest_market_data_fetch_historical_ohlcv_limit,
                    },
                )

    def convert_rest_response_for_create_order(self, *, json_deserialized_payload, rest_request):
        return Order(
            api_method=ApiMethod.REST,
            symbol=rest_request.query_params["symbol"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["updateTime"]),
            order_id=json_deserialized_payload["orderId"],
            client_order_id=rest_request.query_params.get("origClientOrderId"),
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["updateTime"]),
            status=OrderStatus.CREATE_ACKNOWLEDGED,
        )

    def convert_rest_response_for_cancel_order(self, *, json_deserialized_payload, rest_request):
        return Order(
            api_method=ApiMethod.REST,
            symbol=rest_request.query_params["symbol"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["updateTime"]),
            order_id=json_deserialized_payload["orderId"],
            client_order_id=rest_request.query_params.get("origClientOrderId"),
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["updateTime"]),
            status=OrderStatus.CANCEL_ACKNOWLEDGED,
        )

    def convert_rest_response_for_fetch_order(self, *, json_deserialized_payload, rest_request):
        if json_deserialized_payload:
            return self.convert_dict_to_order(input=json_deserialized_payload, api_method=ApiMethod.REST, symbol=x["symbol"])
        else:
            return Order(
                api_method=ApiMethod.REST,
                symbol=rest_request.query_params["symbol"],
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["time"]),
                order_id=rest_request.query_params.get("orderId"),
                client_order_id=rest_request.query_params.get("origClientOrderId"),
                status=OrderStatus.REJECTED,
            )

    def convert_rest_response_for_fetch_open_order(self, *, json_deserialized_payload, rest_request):
        return [self.convert_dict_to_order(input=x, api_method=ApiMethod.REST, symbol=x["symbol"]) for x in json_deserialized_payload]

    def convert_rest_response_for_fetch_open_order_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        pass

    def convert_rest_response_for_fetch_position(self, *, json_deserialized_payload, rest_request):
        result = []
        for x in json_deserialized_payload:
            position_side = x['positionSide']
            position_amount = x['positionAmt']
            is_long = None

            if not Decimal(position_amount).is_zero():
                if pos_side == "LONG":
                    is_long = True
                elif pos_side == "SHORT":
                    is_long = False
                else:
                    is_long = not position_amount.startswith("-")

            result.append(Position(
            api_method=ApiMethod.REST,
            symbol=x["symbol"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["updatedTime"]),
            quantity=remove_leading_negative_sign_in_string(input=position_amount),
            is_long=is_long,
            entry_price=x["entryPrice"],
            mark_price=x["markPrice"],
            initial_margin=x["initialMargin"],
            maintenance_margin=x["maintMargin"],
            unrealized_pnl=x["unRealizedProfit"],
            liquidation_price=x["liquidationPrice"],
        ) )
        return result

    def convert_rest_response_for_fetch_balance(self, *, json_deserialized_payload, rest_request):
        return [Balance(
            api_method=ApiMethod.REST,
            symbol=x["asset"],
            quantity=x["balance"],
        ) for x in json_deserialized_payload]

    def convert_rest_response_for_historical_order(self, *, json_deserialized_payload, rest_request):
        symbol = rest_request.query_params["symbol"]

        return [self.convert_dict_to_order(input=x, api_method=ApiMethod.REST, symbol=symbol) for x in json_deserialized_payload]

    def convert_rest_response_for_historical_order_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        symbol = rest_request.query_params["symbol"]
        data = json_deserialized_payload
        start_time = rest_request.query_params.get("startTime")
        end_time = rest_request.query_params.get("endTime")

        if data:
            query_params = {
                "symbol": symbol,
                "limit": self.rest_account_fetch_historical_order_limit,
            }
            if start_time:
                query_params["startTime"] = start_time

            head = data[0]
            head_ts = int(head['time'])
            tail = data[-1]
            tail_ts = int(tail['time'])

            if head_ts < tail_ts:
                end = head_ts
            else:
                end = tail_ts

            query_params["endTime"] = end

            return self.rest_account_create_get_request_function_with_signature(path=rest_request.path, query_params=query_params)
        elif start_time and (
            self.fetch_historical_order_start_unix_timestamp_seconds is None or start_time > self.fetch_historical_order_start_unix_timestamp_seconds * 1000
        ):
            end_time = start_time - 1
            start_time = max(end_time - 7 * 86400 * 1000, (self.fetch_historical_order_start_unix_timestamp_seconds or 0) * 1000)

            return self.rest_account_create_get_request_function_with_signature(
                path=rest_request.path,
                query_params={
                    "symbol": symbol,
                    "startTime": start_time,
                    "endTime": end_time,
                    "limit": self.rest_account_fetch_historical_order_limit,
                },
            )

    def convert_rest_response_for_historical_fill(self, *, json_deserialized_payload, rest_request):
        symbol = rest_request.query_params["symbol"]

        return [Fill(
            api_method=ApiMethod.REST,
            symbol=symbol,
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["time"]),
            order_id=x.get("orderId"),
            client_order_id=x.get("origClientOrderId"),
            trade_id=x["id"],
            is_buy=x["side"] == "BUY",
            price=x["price"],
            quantity=x["qty"],
            quote_quantity=x['quoteQty'],
            is_maker=x["maker"],
            fee_asset=x.get("commissionAsset"),
            fee_quantity=remove_leading_negative_sign_in_string(input=x["commission"]) if x["commission"] else None,
            is_fee_rebate=x["commission"].startswith("-") if x["commission"] and not Decimal(x['commission']).is_zero() else None,
        ) for x in json_deserialized_payload]

    def convert_rest_response_for_historical_fill_to_next_rest_request_function(self, *, json_deserialized_payload, rest_request):
        symbol = rest_request.query_params["symbol"]
        data = json_deserialized_payload
        start_time = rest_request.query_params.get("startTime")
        end_time = rest_request.query_params.get("endTime")

        if data:
            query_params = {
                "symbol": symbol,
                "limit": self.rest_account_fetch_historical_fill_limit,
            }
            if start_time:
                query_params["startTime"] = start_time

            head = data[0]
            head_ts = int(head['time'])
            tail = data[-1]
            tail_ts = int(tail['time'])

            if head_ts < tail_ts:
                end = head_ts
            else:
                end = tail_ts

            query_params["endTime"] = end

            return self.rest_account_create_get_request_function_with_signature(path=rest_request.path, query_params=query_params)
        elif start_time and (
            self.fetch_historical_fill_start_unix_timestamp_seconds is None or start_time > self.fetch_historical_fill_start_unix_timestamp_seconds * 1000
        ):
            end_time = start_time - 1
            start_time = max(end_time - 7 * 86400 * 1000, (self.fetch_historical_fill_start_unix_timestamp_seconds or 0) * 1000)

            return self.rest_account_create_get_request_function_with_signature(
                path=rest_request.path,
                query_params={
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
                        symbol=rest_response.rest_request.query_params["symbol"],
                        order_id=rest_response.rest_request.query_params.get("orderId"),
                        client_order_id=rest_response.rest_request.query_params.get("origClientOrderId"),
                    )
                except Exception as exception:
                    self.logger.error(exception)

            self.create_task(coro=start_rest_account_fetch_order())

        elif self.is_rest_response_for_fetch_order(rest_response=rest_response):
            if (
                rest_response.status_code == 404
            ):
                now_time_point = time_point_now()
                self.replace_order(
                    symbol=rest_response.rest_request.query_params["symbol"],
                    order_id=rest_response.rest_request.query_params.get("orderId"),
                    client_order_id=rest_response.rest_request.query_params.get("origClientOrderId"),
                    exchange_update_time_point=now_time_point,
                    local_update_time_point=now_time_point,
                    status=OrderStatus.REJECTED,
                )

    async def handle_websocket_on_connected(self, *, websocket_connection):
        if websocket_connection.base_url in (self.websocket_market_data_base_url, self.websocket_account_base_url):
            if websocket_connection.path == self.websocket_market_data_path:
                await self.websocket_market_data_subscribe(websocket_connection=websocket_connection)
            elif websocket_connection.path == self.websocket_account_path:
                async def start_periodic_rest_account_keepalive_user_data_stream():
                    try:
                        while True:
                            await asyncio.sleep(self.rest_account_keepalive_user_data_stream_interval_seconds)
                            rest_request = RestRequest(
                                id=self.generate_next_rest_request_id(), base_url=self.rest_account_base_url, method=RestRequest.METHOD_PUT, path=self.rest_account_start_user_data_stream_path
                            )
                            self.logger.fine("rest_request", rest_request)
                            time_point=time_point_now()
                            self.sign_request(rest_request=rest_request, time_point=time_point)
                            try:
                                async with await self.perform_rest_request(rest_request=rest_request) as client_response:
                                    raw_rest_response = client_response
                                    raw_rest_response_text = await raw_rest_response.text()
                                    self.logger.trace("raw_rest_response_text", raw_rest_response_text)
                                    rest_response = RestResponse(
                                        rest_request=rest_request,
                                        status_code=raw_rest_response.status,
                                        payload=raw_rest_response_text,
                                        headers=raw_rest_response.headers,
                                        json_deserialize=self.json_deserialize,
                                    )
                                    self.logger.fine("rest_response", rest_response)
                            except Exception as exception:
                                self.logger.error(exception)

                    except Exception as exception:
                        self.logger.error(exception)

                self.create_task(coro=start_periodic_rest_account_keepalive_user_data_stream())

        elif websocket_connection.base_url == self.websocket_account_trade_base_url:
            await self.websocket_login(websocket_connection=websocket_connection)


    async def start_websocket_connect_create_url(self, *, base_url, path, query_params):
        if base_url == self.websocket_account_base_url and path == self.websocket_account_path:
            delay_seconds = 0
            while True:
                rest_request = RestRequest(
                    id=self.generate_next_rest_request_id(), base_url=self.rest_account_base_url, method=RestRequest.METHOD_POST, path=self.rest_account_start_user_data_stream_path
                )
                self.logger.fine("rest_request", rest_request)
                time_point=time_point_now()
                self.sign_request(rest_request=rest_request, time_point=time_point)
                try:
                    async with await self.perform_rest_request(rest_request=rest_request) as client_response:
                        raw_rest_response = client_response
                        raw_rest_response_text = await raw_rest_response.text()
                        self.logger.trace("raw_rest_response_text", raw_rest_response_text)
                        rest_response = RestResponse(
                            rest_request=rest_request,
                            status_code=raw_rest_response.status,
                            payload=raw_rest_response_text,
                            headers=raw_rest_response.headers,
                            json_deserialize=self.json_deserialize,
                        )
                        self.logger.fine("rest_response", rest_response)

                        if self.is_rest_response_success(rest_response=rest_response):
                            json_deserialized_payload=rest_response.json_deserialized_payload
                            listen_key = json_deserialized_payload['listenKey']
                            modified_path = path.format(listenKey=listen_key)
                            return create_url(base_url=base_url, path=modified_path)

                except Exception as exception:
                    self.logger.error(exception)

                self.logger.warning(f"delay for {delay_seconds} seconds before start user data stream")
                await asyncio.sleep(delay_seconds)
                delay_seconds = (
                    min(
                        delay_seconds * self.websocket_reconnect_delay_seconds_exponential_backoff_base,
                        self.websocket_reconnect_delay_seconds_exponential_backoff_max,
                    )
                    if delay_seconds > 0
                    else self.websocket_reconnect_delay_seconds_exponential_backoff_initial
                )
        else:
            return await super().start_websocket_connect_create_url(base_url=base_url, path=path, query_params=query_params)


    def websocket_connection_ping_on_application_level_create_websocket_request(self):
        pass

    def websocket_login_create_websocket_request(self, *, time_point):
        id = self.generate_next_websocket_request_id()
        timestamp = int(convert_time_point_to_unix_timestamp_milliseconds(time_point=time_point))

        params = {"apiKey":self.api_key, "timestamp":timestamp,}
        payload = '&'.join([f'{param}={value}' for param, value in sorted(params.items())])
        params['signature'] = base64.b64encode(self.websocket_order_entry_api_private_key.sign(payload.encode('ASCII'))).decode('ASCII')

        payload = self.json_serialize(
            {
                "id": id,
                "method": "session.logon",
                "params": params,
            }
        )

        return self.websocket_create_request(id=id, payload=payload)

    def websocket_market_data_update_subscribe_create_websocket_request(self, *, symbols, is_subscribe):
        if self.subscribe_bbo or self.subscribe_trade or self.subscribe_ohlcv:
            params = []

            for symbol in symbols:
                lower_symbol = symbol.lower()
                if self.subscribe_bbo:
                    params.append(f"{lower_symbol}@{self.websocket_market_data_channel_bbo}")
                if self.subscribe_trade:
                    params.append(f"{lower_symbol}@{self.websocket_market_data_channel_trade}")
                if self.subscribe_ohlcv:
                    params.append(f"{lower_symbol}@{self.websocket_market_data_channel_ohlcv}_{self.convert_ohlcv_interval_seconds_to_string(ohlcv_interval_seconds=self.ohlcv_interval_seconds)}")

            id = self.generate_next_websocket_request_id()
            payload = self.json_serialize({"id": int(id), "method": "SUBSCRIBE", "params": params})
            return self.websocket_create_request(id=id, payload=payload)
        else:
            return None



    def websocket_account_update_subscribe_create_websocket_request(self, *, is_subscribe):
        pass

    def websocket_account_create_order_create_websocket_request(self, *, order):
        id = self.generate_next_websocket_request_id()
        params = self.account_create_order_create_params(order=order)
        return WebsocketRequest(id=id, json_payload={"order.place": id, "method": "order.place", "params": params}, json_serialize=self.json_serialize)

    def websocket_account_cancel_order_create_websocket_request(self, *, symbol, order_id=None, client_order_id=None):
        id = self.generate_next_websocket_request_id()
        params = self.account_cancel_order_create_params(symbol=symbol, order_id=order_id, client_order_id=client_order_id)
        return WebsocketRequest(id=id, json_payload={"id": id, "method": "order.cancel", "params": params}, json_serialize=self.json_serialize)

    def websocket_on_message_extract_data(self, *, websocket_connection, websocket_message):
        json_deserialized_payload = websocket_message.json_deserialized_payload

        websocket_message.payload_summary = {
            "error": json_deserialized_payload.get("error"),
            "status": json_deserialized_payload.get("status"),
            "data,e": json_deserialized_payload.get("data", {}).get("e"),
            "e": json_deserialized_payload.get("e"),
        }

        id = (
            json_deserialized_payload.get("id")
        )
        websocket_message.websocket_request_id = str(id) if id is not None else None

        if websocket_message.websocket_request_id:
            websocket_message.websocket_request = self.websocket_requests.get(websocket_message.websocket_request_id)

        return websocket_message

    def is_websocket_push_data(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        return (websocket_connection.base_url ==self.websocket_market_data_base_url and payload_summary["data,e"] is not None) or (websocket_connection.base_url ==self.websocket_account_base_url and payload_summary["e"] is not None)

    def is_websocket_push_data_for_bbo(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        return websocket_connection.base_url ==self.websocket_market_data_base_url and payload_summary["data,e"]==self.websocket_market_data_channel_bbo

    def is_websocket_push_data_for_trade(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        return websocket_connection.base_url ==self.websocket_market_data_base_url and payload_summary["data,e"]==self.websocket_market_data_channel_trade

    def is_websocket_push_data_for_ohlcv(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        return websocket_connection.base_url ==self.websocket_market_data_base_url and payload_summary["data,e"]==self.websocket_market_data_channel_ohlcv

    def is_websocket_push_data_for_order(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        return websocket_connection.base_url ==self.websocket_account_base_url and payload_summary["e"] == self.websocket_account_channel_order

    def is_websocket_push_data_for_position(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        json_deserialized_payload = websocket_message.json_deserialized_payload
        return websocket_connection.base_url ==self.websocket_account_base_url and payload_summary["e"] == self.websocket_account_channel_position and json_deserialized_payload['a'].get('P')

    def is_websocket_push_data_for_balance(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        json_deserialized_payload = websocket_message.json_deserialized_payload
        return websocket_connection.base_url ==self.websocket_account_base_url and payload_summary["e"] == self.websocket_account_channel_balance and json_deserialized_payload['a'].get('B')

    def is_websocket_push_data_for_system_event(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        return websocket_connection.base_url ==self.websocket_account_base_url and payload_summary["e"] == self.websocket_account_system_event_listen_key_expired


    def is_websocket_response_success(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        if websocket_connection.base_url == self.websocket_market_data_base_url:
            return not payload_summary["error"]
        elif websocket_connection.base_url == self.websocket_account_trade_base_url:
            return not payload_summary["error"]  and payload_summary["status"] and payload_summary["status"] >= 200 and payload_summary["status"] < 300

    def is_websocket_response_for_create_order(self, *, websocket_message):
        websocket_request = websocket_message.websocket_request
        websocket_connection = websocket_message.websocket_connection
        json_deserialized_websocket_request_payload = self.json_deserialize(websocket_request.payload)
        return websocket_connection.base_url == self.websocket_account_trade_base_url and json_deserialized_websocket_request_payload.get('method') == "order.create"

    def is_websocket_response_for_cancel_order(self, *, websocket_message):
        websocket_request = websocket_message.websocket_request
        websocket_connection = websocket_message.websocket_connection
        json_deserialized_websocket_request_payload = self.json_deserialize(websocket_request.payload)
        return websocket_connection.base_url == self.websocket_account_trade_base_url and json_deserialized_websocket_request_payload.get('method') == "order.cancel"

    def is_websocket_response_for_subscribe(self, *, websocket_message):
        websocket_request = websocket_message.websocket_request
        websocket_connection = websocket_message.websocket_connection
        json_deserialized_websocket_request_payload = self.json_deserialize(websocket_request.payload)
        return websocket_connection.base_url == self.websocket_market_data_base_url and json_deserialized_websocket_request_payload.get('method') == "SUBSCRIBE"

    def is_websocket_response_for_login(self, *, websocket_message):
        websocket_request = websocket_message.websocket_request
        websocket_connection = websocket_message.websocket_connection
        json_deserialized_websocket_request_payload = self.json_deserialize(websocket_request.payload)
        return websocket_connection.base_url == self.websocket_account_trade_base_url and json_deserialized_websocket_request_payload.get('method') == "session.logon"

    async def handle_websocket_push_data_for_system_event(self, *, websocket_message):
        websocket_connection = websocket_message.websocket_connection
        payload_summary = websocket_message.payload_summary
        if websocket_connection.base_url == self.websocket_account_base_url and websocket_connection.path == self.websocket_account_path and payload_summary["data,e"] == self.websocket_account_system_event_listen_key_expired:
            await websocket_message.websocket_connection.close()

    def convert_websocket_push_data_for_bbo(self, *, json_deserialized_payload):
        symbol = json_deserialized_payload["s"]

        return [
            Bbo(
                api_method=ApiMethod.WEBSOCKET,
                symbol=symbol,
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["T"]),
                best_bid_price=json_deserialized_payload["b"] if json_deserialized_payload.get("b") else None,
                best_bid_size=json_deserialized_payload["B"] if json_deserialized_payload.get("B") else None,
                best_ask_price=json_deserialized_payload["a"] if json_deserialized_payload.get("a") else None,
                best_ask_size=json_deserialized_payload["A"] if json_deserialized_payload.get("A") else None,
            )
        ]

    def convert_websocket_push_data_for_trade(self, *, json_deserialized_payload):
        symbol = json_deserialized_payload["s"]

        return [
            Trade(
                api_method=ApiMethod.WEBSOCKET,
                symbol=symbol,
                exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["T"]),
                trade_id=str(json_deserialized_payload["t"]),
                price=json_deserialized_payload["p"],
                size=json_deserialized_payload["q"],
                is_buyer_maker=json_deserialized_payload["m"],
            )
        ]



    def convert_websocket_push_data_for_order(self, *, json_deserialized_payload):
        o = json_deserialized_payload['o']
        status = self.order_status_mapping[o["X"]]
        exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["T"])
        return [Order(
            api_method=ApiMethod.WEBSOCKET,
            symbol=o['s'],
            exchange_update_time_point=exchange_update_time_point,
            order_id=str(o["i"]),
            is_buy=o["S"] == "BUY",
            price=o["p"],
            quantity=o["q"],
            is_market=o["o"] == "MARKET",
            is_post_only=o["f"] == "GTX",
            is_fok=o["f"] == "FOK",
            is_ioc=o["f"] == "IOC",
            is_reduce_only=o["R"],
            cumulative_filled_quantity=o["z"],
            average_filled_price=o['ap'],
            exchange_create_time_point=exchange_update_time_point if status==OrderStatus.NEW else None,
            status=status,
        )]

    def convert_websocket_push_data_for_fill(self, *, json_deserialized_payload):
        o = json_deserialized_payload['o']
        return [Fill(
            api_method=ApiMethod.Websocket,
            symbol=o['s'],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["T"]),
            order_id=str(o["i"]),
            trade_id=o["t"],
            is_buy=o["S"] == "BUY",
            price=o["L"],
            quantity=o["l"],
            is_maker=o["m"],
            fee_asset=o['N'],
            fee_quantity=remove_leading_negative_sign_in_string(input=x["n"]),
            is_fee_rebate=x["n"].startswith("-") if not Decimal(x['n']).is_zero() else None,
        )]

    def convert_websocket_push_data_for_position(self, *, json_deserialized_payload):
        result = []
        for x in json_deserialized_payload["a"]['P']:
            position_side = x['ps']
            position_amount = x['pa']
            is_long = None

            if not Decimal(position_amount).is_zero():
                if pos_side == "LONG":
                    is_long = True
                elif pos_side == "SHORT":
                    is_long = False
                else:
                    is_long = not position_amount.startswith("-")
            result.append(Position(
            api_method=ApiMethod.Websocket,
            symbol=x["s"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=json_deserialized_payload["T"]),
            quantity=remove_leading_negative_sign_in_string(input=position_amount),
            is_long=is_long,
            entry_price=x["ep"],
            unrealized_pnl=x["up"],
        ))
        return result

    def convert_websocket_push_data_for_balance(self, *, json_deserialized_payload):
        return [Balance(
            api_method=ApiMethod.Websocket,
            symbol=x["a"],
            quantity=x["wb"],
        ) for x in json_deserialized_payload["a"]["B"]]

    def convert_websocket_response_for_create_order(self, *, json_deserialized_payload, websocket_request):
        result = json_deserialized_payload["result"]
        exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(
                unix_timestamp_milliseconds=result["updateTime"]
            )

        return Order(
            api_method=ApiMethod.WEBSOCKET,
            symbol=result["symbol"],
            exchange_update_time_point=exchange_update_time_point,
            order_id=str(result["orderId"]),
            client_order_id=websocket_request.json_payload["params"].get("newClientOrderId"),
            exchange_create_time_point=exchange_update_time_point,
            status=OrderStatus.CREATE_ACKNOWLEDGED,
        )

    def convert_websocket_response_for_cancel_order(self, *, json_deserialized_payload, websocket_request):
        result = json_deserialized_payload["result"]

        return Order(
            api_method=ApiMethod.WEBSOCKET,
            symbol=result["symbol"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(
                unix_timestamp_milliseconds=result["updateTime"]
            ),
            order_id=str(result["orderId"]),
            client_order_id=websocket_request.json_payload["params"].get("origClientOrderId"),
            status=OrderStatus.CANCEL_ACKNOWLEDGED,
        )

    async def handle_websocket_push_data_for_order(self, *, websocket_message):

        if self.subscribe_order:
            await super().handle_websocket_push_data_for_order(websocket_message=websocket_message)

        if self.subscribe_fill:
            await super().handle_websocket_push_data_for_fill(websocket_message=websocket_message)

    async def handle_websocket_push_data_for_balance(self, *, websocket_message):

        if self.subscribe_balance:
            await super().handle_websocket_push_data_for_balance(websocket_message=websocket_message)

        if self.subscribe_position:
            await super().handle_websocket_push_data_for_position(websocket_message=websocket_message)

    async def handle_websocket_response_for_error(self, *, websocket_message):
        self.logger.warning("websocket_message", websocket_message)

        if self.is_websocket_response_for_create_order(websocket_message=websocket_message) or self.is_websocket_response_for_cancel_order(
            websocket_message=websocket_message
        ):

            async def start_rest_account_fetch_order():
                try:
                    await self.rest_account_fetch_order(
                        symbol=websocket_message.websocket_request.json_payload["params"]["symbol"],
                        order_id=websocket_message.websocket_request.json_payload["params"].get("orderId"),
                        client_order_id=websocket_message.websocket_request.json_payload["params"].get("newClientOrderId" if self.is_websocket_response_for_create_order(websocket_message=websocket_message) else "origClientOrderId"),
                    )
                except Exception as exception:
                    self.logger.error(exception)

            self.create_task(coro=start_rest_account_fetch_order())

    def account_create_order_create_params(self, *, order):
        if order.is_post_only:
            time_in_force = "GTX"
        elif order.is_fok:
            time_in_force = "FOK"
        elif order.is_ioc:
            time_in_force = "IOC"
        else:
            time_in_force = "GTC"

        json_payload = {
            "symbol": order.symbol,
            "newClientOrderId": order.client_order_id,
            "side": "BUY" if order.is_buy else "Sell",
            "type": "MARKET" if order.is_market else "LIMIT",
            "quantity": order.quantity,
            "timeInForce": time_in_force,
        }
        if order.price:
            json_payload["price"] = order.price
        if order.is_reduce_only:
            json_payload["reduceOnly"] = "true"
        if order.extra_params:
            json_payload.update(order.extra_params)

        return json_payload

    def account_cancel_order_create_params(self, *, symbol, order_id=None, client_order_id=None):
        json_payload = {
            "symbol": symbol,
        }
        if order_id:
            json_payload["orderId"] = order_id
        else:
            json_payload["origClientOrderId"] = client_order_id
        return json_payload

    def convert_dict_to_order(self, *, input, api_method, symbol):
        return Order(
            api_method=api_method,
            symbol=symbol,
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["updatedTime"]),
            order_id=str(input.get("orderId")) if input.get("orderId") else None,
            client_order_id=input.get("origClientOrderId"),
            is_buy=input["side"] == "BUY",
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

    def convert_ohlcv_interval_seconds_to_string(self, *, ohlcv_interval_seconds):
            if ohlcv_interval_seconds < 3600:
                return f"{ohlcv_interval_seconds//60}m"
            elif ohlcv_interval_seconds < 86400:
                return f"{ohlcv_interval_seconds//3600}h"
            elif ohlcv_interval_seconds < 604800:
                return f"{ohlcv_interval_seconds//86400}d"
            else:
                return "1w"
