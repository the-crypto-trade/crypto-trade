from crypto_trade.exchange_api import ApiMethod, Ohlcv
from crypto_trade.exchanges.delegates.binance_futures_base import BinanceFuturesBase


class BinanceUsdsMarginedFutures(BinanceFuturesBase):

    def __init__(self, **kwargs) -> None:
        super().__init__(**kwargs)

        self.rest_market_data_base_url = "https://fapi.binance.com"
        if self.is_paper_trading:
            self.rest_market_data_base_url = "https://testnet.binancefuture.com"
        self.rest_account_base_url = self.rest_market_data_base_url
        self.rest_market_data_fetch_all_instrument_information_path = "/fapi/v1/exchangeInfo"
        self.rest_market_data_fetch_bbo_path = "/fapi/v1/ticker/bookTicker"
        self.rest_market_data_fetch_historical_trade_path = "/fapi/v1/historicalTrades"
        self.rest_market_data_fetch_historical_trade_limit = 500
        self.rest_market_data_fetch_historical_ohlcv_path = "/fapi/v1/klines"
        self.rest_market_data_fetch_historical_ohlcv_limit = 1500
        self.rest_account_create_order_path = "/fapi/v1/order"
        self.rest_account_cancel_order_path = "/fapi/v1/order"
        self.rest_account_fetch_order_path = "/fapi/v1/order"
        self.rest_account_fetch_open_order_path = "/fapi/v1/openOrders"
        self.rest_account_fetch_position_path = "/fapi/v3/positionRisk"
        self.rest_account_fetch_balance_path = "/fapi/v3/balance"
        self.rest_account_fetch_historical_order_path = "/fapi/v1/allOrders"
        self.rest_account_fetch_historical_order_limit = 1000
        self.rest_account_fetch_historical_fill_path = "/fapi/v1/userTrades"
        self.rest_account_fetch_historical_fill_limit = 1000

        self.websocket_market_data_base_url = "wss://fstream.binance.com"
        if self.is_paper_trading:
            self.websocket_market_data_base_url = "wss://fstream.binancefuture.com"
        self.websocket_account_base_url = self.websocket_market_data_base_url
        self.websocket_market_data_path = "/stream"
        self.websocket_market_data_channel_bbo = "bookTicker"
        self.websocket_market_data_channel_trade = "trade"
        self.websocket_market_data_channel_ohlcv = "kline"
        self.websocket_account_path = "/ws/{listenKey}"
        self.websocket_account_channel_order = "ORDER_TRADE_UPDATE"
        self.websocket_account_channel_fill = "ORDER_TRADE_UPDATE"
        self.websocket_account_channel_balance = "ACCOUNT_UPDATE"
        self.websocket_account_trade_base_url = "wss://ws-fapi.binance.com"
        if self.is_paper_trading:
            self.websocket_market_data_base_url = "wss://testnet.binancefuture.com"
        self.websocket_account_trade_path = "/ws-fapi/v1"

        self.rest_account_start_user_data_stream_path = "/fapi/v1/listenKey"
        self.rest_account_keepalive_user_data_stream_path = "/fapi/v1/listenKey"

    def convert_websocket_push_data_for_ohlcv(self, *, json_deserialized_payload):
        data = json_deserialized_payload["data"]
        symbol = data["s"]
        k = data["k"]

        return [
            Ohlcv(
                api_method=ApiMethod.WEBSOCKET,
                symbol=symbol,
                start_unix_timestamp_seconds=int(k["t"]) // 1000,
                open_price=k["o"],
                high_price=k["h"],
                low_price=k["l"],
                close_price=k["c"],
                volume=k["v"],
                base_volume=k["v"],
                quote_volume=k["q"],
            )
        ]
