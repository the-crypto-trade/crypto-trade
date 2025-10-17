import asyncio
import base64
import hashlib
import hmac
from datetime import datetime, timezone
from decimal import Decimal
from hyperliquid.utils.signing import sign_l1_action
import eth_account
from eth_account.signers.local import LocalAccount
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
    datetime_format_3,
    normalize_decimal_string,
    remove_leading_negative_sign_in_string,
    time_point_now,
    convert_num_decimals_to_string,
)


class HyperliquidInstrumentType(StrEnum):
    SPOT = "SPOT"
    PERPETUALS = "PERP"



class Hyperliquid(Exchange):

    def __init__(self, **kwargs) -> None:
        super().__init__(name="hyperliquid", **kwargs)

        self.rest_market_data_base_url = "https://api.hyperliquid.xyz"
        self.rest_account_base_url = self.rest_market_data_base_url
        self.rest_market_data_fetch_all_instrument_information_path = "/info"
        self.rest_market_data_fetch_bbo_path = "/info"
        self.rest_account_create_order_path = "/exchange"
        self.rest_account_cancel_order_path = "/exchange"
        self.rest_account_fetch_order_path = "/info"
        self.rest_account_fetch_open_order_path = "/info"
        self.rest_account_fetch_position_path = "/info"
        self.rest_account_fetch_balance_path = "/info"
        self.rest_account_fetch_historical_order_path = "/info"
        self.rest_account_fetch_historical_fill_path = "/info"

        self.websocket_market_data_base_url = "wss://api.hyperliquid.xyz"
        if self.is_paper_trading:
            self.websocket_market_data_base_url = "wss://api.hyperliquid-testnet.xyz"
        self.websocket_account_base_url = self.websocket_market_data_base_url
        self.websocket_market_data_path = "/ws"
        self.websocket_market_data_channel_bbo = "bbo"
        self.websocket_market_data_channel_trade = "trades"
        self.websocket_market_data_channel_ohlcv = "candle"
        self.websocket_account_path = "/ws"
        self.websocket_account_channel_order = "orderUpdates"
        self.websocket_account_channel_fill = 'userFills'
        self.websocket_account_channel_position = "activeAssetData"
        self.websocket_account_channel_balance = "activeAssetCtx"
        self.websocket_account_trade_base_url = self.websocket_account_base_url
        self.websocket_account_trade_path = self.websocket_account_path

        self.order_status_mapping = {
            "open": OrderStatus.NEW,
            "filled": OrderStatus.FILLED,
            "canceled": OrderStatus.CANCELED,
            "triggered": OrderStatus.OPEN,
            "rejected": OrderStatus.REJECTED,
            "marginCanceled": OrderStatus.CANCELED,
            "vaultWithdrawalCanceled": OrderStatus.CANCELED,
            "openInterestCapCanceled": OrderStatus.CANCELED,
            "selfTradeCanceled": OrderStatus.CANCELED,
            "reduceOnlyCanceled": OrderStatus.CANCELED,
            "siblingFilledCanceled": OrderStatus.CANCELED,
            "delistedCanceled": OrderStatus.CANCELED,
            "liquidatedCanceled": OrderStatus.CANCELED,
            "scheduledCancel": OrderStatus.CANCELED,
            "tickRejected": OrderStatus.REJECTED,
            "minTradeNtlRejected": OrderStatus.REJECTED,
            "perpMarginRejected": OrderStatus.REJECTED,
            "reduceOnlyRejected": OrderStatus.REJECTED,
            "badAloPxRejected": OrderStatus.REJECTED,
            "iocCancelRejected": OrderStatus.REJECTED,
            "badTriggerPxRejected": OrderStatus.REJECTED,
            "marketOrderNoLiquidityRejected": OrderStatus.REJECTED,
            "positionIncreaseAtOpenInterestCapRejected": OrderStatus.REJECTED,
            "positionFlipAtOpenInterestCapRejected": OrderStatus.REJECTED,
            "tooAggressiveAtOpenInterestCapRejected": OrderStatus.REJECTED,
            "openInterestIncreaseRejected": OrderStatus.REJECTED,
            "insufficientSpotBalanceRejected": OrderStatus.REJECTED,
            "oracleRejected": OrderStatus.REJECTED,
            "perpMaxPositionRejected": OrderStatus.REJECTED,
        }

        if self.instrument_type == HyperliquidInstrumentType.SPOT:
            self.subscribe_position = False
            self.rest_account_fetch_position_period_seconds = None

        self.rest_request_headers = {
            'Content-Type':'application/json',
        }

        self.spot_base_asset_to_index_mapping = {}
        self.spot_index_to_base_asset_mapping = {}

        self.account_address = '0x0000000000000000000000000000000000000000'
        if self.api_key:
            self.account_address = eth_account.Account.from_key(self.api_key).address

        self.spot_quote_asset = "USDC"
        self.perp_quote_asset = "USDT"

    def is_instrument_type_valid(self, *, instrument_type):
        return instrument_type in (
            HyperliquidInstrumentType.SPOT,
            HyperliquidInstrumentType.PERP,
        )

    def convert_base_asset_quote_asset_to_symbol(self, *, base_asset, quote_asset):
        if self.instrument_type == HyperliquidInstrumentType.SPOT:
            return f"{base_asset}-SPOT"
        elif self.instrument_type == HyperliquidInstrumentType.PERP:
            return f"{base_asset}-PERP"
        else:
            return None

    def convert_symbol_to_coin(self, *, symbol):
        base_asset = symbol.split('-')[0]
        if self.instrument_type == HyperliquidInstrumentType.SPOT:
            return f'@{self.spot_base_asset_to_index_mapping[base_asset]}'
        else:
            return base_asset

    def rest_market_data_fetch_all_instrument_information_create_rest_request_function(self):
        return self.rest_market_data_create_post_request_function(
            path=self.rest_market_data_fetch_all_instrument_information_path,
            json_payload={
                'type':'spotMeta' if self.instrument_type==HyperliquidInstrumentType.SPOT else 'meta',
            },
            json_serialize=self.json_serialize,
            headers=self.rest_request_headers,
        )

    async def rest_market_data_fetch_bbo(self):
        for symbol in symbols:
            await self.send_rest_request(rest_request_function=self.rest_market_data_fetch_bbo_create_rest_request_function_for_symbol(symbol=symbol))
            await asyncio.sleep(self.rest_market_data_fetch_all_instrument_information_period_seconds)

    def rest_market_data_fetch_bbo_create_rest_request_function_for_symbol(self, *, symbol):
        return self.rest_market_data_create_post_request_function(
            path=self.rest_market_data_fetch_bbo_path,
            json_payload={
                'type':'l2Book',
                'coin':self.convert_symbol_to_coin(symbol=symbol),
            },
            json_serialize=self.json_serialize,
            headers=self.rest_request_headers,
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
        return self.rest_account_create_post_request_function(
            path=self.rest_account_fetch_bbo_path,
            json_payload={
                'type':'orderStatus',
                'user':self.account_address,
                'oid':int(order_id) or client_order_id,
            },
            json_serialize=self.json_serialize,
            headers=self.rest_request_headers,
        )

    def rest_account_fetch_open_order_create_rest_request_function(self):
        return self.rest_accountcreate_post_request_function(
            path=self.rest_account_fetch_open_order_path,
            json_payload={
                'type':'openOrders',
                'user':self.account_address,
            },
            json_serialize=self.json_serialize,
            headers=self.rest_request_headers,
        )

    def rest_account_fetch_position_create_rest_request_function(self):
        return self.rest_accountcreate_post_request_function(
            path=self.rest_account_fetch_position_path,
            json_payload={
                'type':'clearinghouseState',
                'user':self.account_address,
            },
            json_serialize=self.json_serialize,
            headers=self.rest_request_headers,
        )

    def rest_account_fetch_balance_create_rest_request_function(self):
        return self.rest_accountcreate_post_request_function(
            path=self.rest_account_fetch_position_path,
            json_payload={
                'type':'spotClearinghouseState' if self.instrument_type == HyperliquidInstrumentType.SPOT else 'clearinghouseState',
                'user':self.account_address,
            },
            json_serialize=self.json_serialize,
            headers=self.rest_request_headers,
        )

    def rest_account_fetch_historical_order_create_rest_request_function(self, *, symbol):
        return self.rest_accountcreate_post_request_function(
            path=self.rest_account_fetch_position_path,
            json_payload={
                'type':'historicalOrders',
                'user':self.account_address,
            },
            json_serialize=self.json_serialize,
            headers=self.rest_request_headers,
        )

    def rest_account_fetch_historical_fill_create_rest_request_function(self, *, symbol):
        return self.rest_accountcreate_post_request_function(
            path=self.rest_account_fetch_position_path,
            json_payload={
                'type':'userFills',
                'user':self.account_address,
            },
            json_serialize=self.json_serialize,
            headers=self.rest_request_headers,
        )

    def is_rest_response_success(self, *, rest_response):
        has_error = rest_response.json_deserialized_payload and 'response' in rest_response.json_deserialized_payload and 'data' in rest_response.json_deserialized_payload['response'] and 'statuses' in rest_response.json_deserialized_payload['response']['data'] \
        and any("error" in x for x in rest_response.json_deserialized_payload['response']['data']['statuses'])
        return (
            super().is_rest_response_success(rest_response=rest_response)
            and not has_error
        )

    def is_rest_response_for_all_instrument_information(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('type') == ('spotMeta' if self.instrument_type==HyperliquidInstrumentType.SPOT else 'meta')

    def is_rest_response_for_bbo(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('type') == 'l2Book'

    def is_rest_response_for_create_order(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('action', {}).get('type') == 'order'

    def is_rest_response_for_cancel_order(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('action', {}).get('type') == 'cancel'

    def is_rest_response_for_fetch_order(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('type') == 'orderStatus'

    def is_rest_response_for_fetch_open_order(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('type') == 'openOrders'

    def is_rest_response_for_fetch_position(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('type') == 'clearinghouseState'

    def is_rest_response_for_fetch_balance(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('type') == ('spotClearinghouseState' if self.instrument_type == HyperliquidInstrumentType.SPOT else 'clearinghouseState')

    def is_rest_response_for_historical_order(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('type') == 'historicalOrders'

    def is_rest_response_for_historical_fill(self, *, rest_response):
        return rest_response.rest_request.json_payload.get('type') == 'userFills'

    def convert_rest_response_for_all_instrument_information(self, *, json_deserialized_payload, rest_request):
        result = []

        if self.instrument_type == HyperliquidInstrumentType.SPOT:
            sz_decimals_by_index = {}
            for x in json_deserialized_payload["tokens"]:
                sz_decimals_by_index[x['index']] = x['szDecimals']
                name = x['name']
                index = x['index']
                self.spot_base_asset_to_index_mapping[name] = index
                self.spot_index_to_base_asset_mapping[index] = name

            for x in json_deserialized_payload["universe"]:
                index = x['index']
                base_asset = self.spot_index_to_base_asset_mapping[index]
                increment_str = convert_num_decimals_to_string(sz_decimals_by_index[index])
                result.append(
                    InstrumentInformation(
                        api_method=ApiMethod.REST,
                        symbol=f'{base_asset}-SPOT',
                        base_asset=base_asset,
                        quote_asset=self.spot_quote_asset,
                        order_price_increment=increment_str,
                        order_quantity_increment=increment_str,
                        order_quantity_min=increment_str,
                    )
                )
        else:
            for x in json_deserialized_payload["universe"]:
                name = x['name']
                sz_decimals = x['szDecimals']
                increment_str = convert_num_decimals_to_string(sz_decimals)
                result.append(
                    InstrumentInformation(
                        api_method=ApiMethod.REST,
                        symbol=f'{name}-PERP',
                        base_asset=name,
                        quote_asset=self.perp_quote_asset,
                        order_price_increment=increment_str,
                        order_quantity_increment=increment_str,
                        order_quantity_min=increment_str,
                    )
                )

        return result

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

    def convert_rest_response_for_create_order(self, *, json_deserialized_payload, rest_request):
        x = json_deserialized_payload["data"][0]

        return Order(
            api_method=ApiMethod.REST,
            symbol=rest_request.json_payload["instId"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
            order_id=x["ordId"],
            client_order_id=x["clOrdId"],
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
            status=OrderStatus.CREATE_ACKNOWLEDGED,
        )

    def convert_rest_response_for_cancel_order(self, *, json_deserialized_payload, rest_request):
        x = json_deserialized_payload["data"][0]

        return Order(
            api_method=ApiMethod.REST,
            symbol=rest_request.json_payload["instId"],
            exchange_update_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=x["ts"]),
            order_id=x["ordId"],
            client_order_id=x["clOrdId"],
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

            return self.rest_account_create_get_request_function_with_signature(
                path=self.rest_account_fetch_open_order_path,
                 "after": after, "limit": self.rest_account_fetch_open_order_limit},
            )

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
                    path=rest_request.path,
                    query_params={
                        "instType": f"{self.instrument_type}",
                        "instId": rest_request.query_params["instId"],
                        "after": after,
                        "limit": self.rest_account_fetch_historical_order_limit,
                    },
                )
        elif rest_request.path == self.rest_account_fetch_historical_order_path:
            query_params = {
                "instType": f"{self.instrument_type}",
                "instId": rest_request.query_params["instId"],
                "limit": self.rest_account_fetch_historical_order_limit,
            }

            if "after" in rest_request.query_params:
                query_params["after"] = rest_request.query_params["after"]

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
                    path=rest_request.path,
                    query_params={
                        "instType": f"{self.instrument_type}",
                        "instId": rest_request.query_params["instId"],
                        "after": after,
                        "limit": self.rest_account_fetch_historical_fill_limit,
                    },
                )
        elif rest_request.path == self.rest_account_fetch_historical_fill_path:
            query_params = {
                "instType": f"{self.instrument_type}",
                "instId": rest_request.query_params["instId"],
                "limit": self.rest_account_fetch_historical_fill_limit,
            }

            if "after" in rest_request.query_params:
                query_params["after"] = rest_request.query_params["after"]

            return self.rest_account_create_get_request_function_with_signature(path=self.rest_account_fetch_historical_fill_path_2, query_params=query_params)

    async def handle_rest_response_for_error(self, *, rest_response):
        self.logger.warning("rest_response", rest_response)

        if self.is_rest_response_for_create_order(rest_response=rest_response) or self.is_rest_response_for_cancel_order(rest_response=rest_response):

            async def start_rest_account_fetch_order():
                try:
                    await self.rest_account_fetch_order(
                        symbol=rest_response.rest_request.json_payload["instId"],
                        order_id=rest_response.rest_request.json_payload.get("ordId"),
                        client_order_id=rest_response.rest_request.json_payload.get("clOrdId"),
                    )
                except Exception as exception:
                    self.logger.error(exception)

            self.create_task(coro=start_rest_account_fetch_order())

        elif self.is_rest_response_for_fetch_order(rest_response=rest_response):
            if (
                rest_response.status_code == 200
                and rest_response.json_deserialized_payload
                and rest_response.json_deserialized_payload.get("code") in ("51001", "51603")
            ):
                now_time_point = time_point_now()
                self.replace_order(
                    symbol=rest_response.rest_request.query_params["instId"],
                    order_id=rest_response.rest_request.query_params.get("ordId"),
                    client_order_id=rest_response.rest_request.query_params.get("clOrdId"),
                    exchange_update_time_point=now_time_point,
                    local_update_time_point=now_time_point,
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
                        base_url=self.websocket_market_data_base_url_2,
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
        self.logger.trace("send application level ping")
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
        else:
            self.logger.trace("received application level pong")

    def websocket_on_message_extract_data(self, *, websocket_connection, websocket_message):
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
                best_bid_price=x["bids"][0][0] if x.get("bids") else None,
                best_bid_size=x["bids"][0][1] if x.get("bids") else None,
                best_ask_price=x["asks"][0][0] if x.get("asks") else None,
                best_ask_size=x["asks"][0][1] if x.get("asks") else None,
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

            async def start_rest_account_fetch_order():
                try:
                    await self.rest_account_fetch_order(
                        symbol=websocket_message.websocket_request.json_payload["args"][0]["instId"],
                        order_id=websocket_message.websocket_request.json_payload["args"][0].get("ordId"),
                        client_order_id=websocket_message.websocket_request.json_payload["args"][0].get("clOrdId"),
                    )
                except Exception as exception:
                    self.logger.error(exception)

            self.create_task(coro=start_rest_account_fetch_order())

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
            "tdMode": order.margin_type.value.lower() if order.margin_type else "cash",
            "clOrdId": order.client_order_id,
            "side": "buy" if order.is_buy else "sell",
            "ordType": ord_type,
            "sz": order.quantity,
            "tag": self.api_broker_id,
        }
        if order.price:
            json_payload["px"] = order.price
        if order.is_reduce_only:
            json_payload["reduceOnly"] = True
        if self.margin_asset or order.margin_asset:
            json_payload["ccy"] = self.margin_asset or order.margin_asset
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
            base_volume=input[5] if self.instrument_type in (HyperliquidInstrumentType.SPOT, HyperliquidInstrumentType.MARGIN) else input[6],
            quote_volume=input[7],
        )

    def convert_dict_to_order(self, *, input, api_method, symbol):
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
            margin_asset=input["ccy"],
            cumulative_filled_quantity=input["accFillSz"] or None,
            average_filled_price=input["avgPx"] if input["avgPx"] else None,
            exchange_create_time_point=convert_unix_timestamp_milliseconds_to_time_point(unix_timestamp_milliseconds=input["cTime"]),
            status=self.order_status_mapping.get(input["state"]),
        )

    def convert_dict_to_fill(self, *, input, api_method, symbol):
        fill_fee = input.get("fillFee", input.get("fee"))
        fill_fee_ccy = input.get("fillFeeCcy", input.get("feeCcy"))
        is_fee_rebate = not fill_fee.startswith("-") if fill_fee and not Decimal(fill_fee).is_zero() else None

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
            is_maker=input["execType"] == "M" if input.get("execType") else None,
            fee_asset=fill_fee_ccy,
            fee_quantity=remove_leading_negative_sign_in_string(input=fill_fee) if fill_fee else None,
            is_fee_rebate=is_fee_rebate,
        )

    def convert_dict_to_position(self, *, input, api_method):
        pos_side = input["posSide"]
        pos = input["pos"]
        symbol = input["instId"]
        is_long = None

        if not Decimal(pos).is_zero():
            if pos_side == "long":
                is_long = True
            elif pos_side == "short":
                is_long = False
            else:
                if (
                    self.instrument_type == HyperliquidInstrumentType.FUTURES
                    or self.instrument_type == HyperliquidInstrumentType.SWAP
                    or self.instrument_type == HyperliquidInstrumentType.OPTION
                ):
                    is_long = not pos.startswith("-")
                elif self.instrument_type == HyperliquidInstrumentType.MARGIN:
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
            leverage=input["lever"],
            initial_margin=input["imr"],
            maintenance_margin=input["mmr"],
            unrealized_pnl=input["upl"],
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
            return f"{ohlcv_interval_seconds//3600}H" + ("utc" if ohlcv_interval_seconds >= 21600 and self.is_ohlcv_interval_aligned_to_utc else "")
        else:
            return f"{ohlcv_interval_seconds//86400}D" + ("utc" if self.is_ohlcv_interval_aligned_to_utc else "")
