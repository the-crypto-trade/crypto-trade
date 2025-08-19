import asyncio
import os
import pprint
import sys
import time
import traceback
import itertools
import statistics
import math

from crypto_trade.exchange_api import Order
from crypto_trade.exchanges.bybit import Bybit, BybitInstrumentType
from crypto_trade.utility import Logger, LogLevel

from crypto_trade.exchanges.okx import Okx, OkxInstrumentType

from crypto_trade.utility import (
    LoggerWithWriter,
    LogLevel,
    RestRequest,
    RestResponse,
    WebsocketConnection,
    WebsocketMessage,
    WebsocketRequest,
    Writer,
    convert_decimal_to_string,
    convert_time_point_delta_to_seconds,
    convert_time_point_to_datetime,
    datetime_format_1,
    round_down,
    round_to_nearest,
    round_up,
    time_point_now,
    time_point_subtract,
)

def generate_log_message_prefix(*, symbol):
    return f"[{symbol}] "

async def main():
    try:
        # Default log level is WARNING. Here is how to change it:
        writer = Writer(end="\n", write_dir='/'.join(os.getenv("LOG_DIR"), os.path.splitext(os.path.basename(__file__))[0]), write_buffering=1)
        logger = LoggerWithWriter(level=getattr(LogLevel, os.getenv("LOG_LEVEL", "WARNING")), name="", writer=writer)

        exchange_name = os.getenv("EXCHANGE_NAME", "bybit").lower()

        instrument_type = {
            "bybit": BybitInstrumentType,
            "okx": OkxInstrumentType,
        }[
            exchange_name
        ]['SPOT']

        exchange_class = {
            "bybit": Bybit,
            "okx": Okx,
        }[exchange_name]

        base_assets = [x.strip() for x in os.getenv("BASE_ASSETS", "BTC,ETH").split(',')]
        quote_asset = os.getenv("QUOTE_ASSET", "USDT").upper()

        symbols = []
        symbol_to_base_asset = {}

        for base_asset in base_assets:
            if exchange_name == 'bybit':
                symbol = f'{base_asset}{quote_asset}'
            elif exchange_name == 'okx':
                symbol = f'{base_asset}-{quote_asset}'
            symbols.append(symbol)
            symbol_to_base_asset[symbol] = base_asset

        base_asset_weights_as_string = os.getenv("BASE_ASSET_WEIGHTS")

        if base_asset_weights_as_string:
            base_asset_weights = [float(x.strip()) for x in base_asset_weights_as_string.split(',')]
            base_asset_normalized_weights = { x[0], x[1] / sum(base_asset_weights) for x in zip(base_assets, base_asset_weights) }
        else:
            base_asset_normalized_weights = { base_asset, 1 / len(base_assets) for base_asset in base_assets }

        unix_timestamp_seconds = int(time.time())

        price_change_look_back_seconds = int(os.getenv('PRICE_CHANGE_LOOK_BACK_SECONDS','18000'))

        class DerivedExchangeClass(exchange_class):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.price_changes = {}
                for symbol in symbols:
                    price_changes[symbol] = []
                self.previous_bbos = None
                self.volatilities = {}
                for symbol in symbols:
                    self.volatilities[symbol] = 0.05

            async def update_websocket_push_data_for_fill(self, *, fills):
                if fills:
                    self.logger.info("fills", fills)
                    for fill in fills:
                        symbol = fill.symbol
                        await self.cancel_orders(symbol=symbol)

        exchange = DerivedExchangeClass(
            symbols=symbols
            subscribe_bbo=True,
            ohlcv_interval_seconds=60,
            fetch_historical_ohlcv_at_start=True,
            fetch_historical_ohlcv_start_unix_timestamp_seconds=unix_timestamp_seconds-historical_prices_look_back_seconds,
            fetch_historical_ohlcv_end_unix_timestamp_seconds=unix_timestamp_seconds,
            subscribe_order=True,
            subscribe_fill=True,
            subscribe_balance=True,
            is_paper_trading=os.getenv("IS_PAPER_TRADING", "false").lower() == 'true',
            api_key=os.getenv("BYBIT_API_KEY", ""),
            api_secret=os.getenv("BYBIT_API_SECRET", ""),
            api_passphrase=os.getenv("BYBIT_API_PASSPHRASE", ""),
            logger=logger,
            start_wait_seconds=float(os.getenv("START_WAIT_SECONDS", "1")),  # increase this value if connecting through a slow VPN
        )

        await exchange.start()

        for symbol, ohlcvs_for_symbol in exchange.ohlcvs.items():
            for pairwise_ohlcvs_for_symbol in itertools.pairwise(ohlcvs_for_symbol):
                first_ohlcv = pairwise_ohlcvs_for_symbol[0]
                second_ohlcv = pairwise_ohlcvs_for_symbol[1]
                first_price = statistics.geometric_mean(first_ohlcv.open_price_as_float(), first_ohlcv.high_price_as_float(),first_ohlcv.low_price_as_float(),first_ohlcv.close_price_as_float())
                second_price = statistics.geometric_mean(second_ohlcv.open_price_as_float(), second_ohlcv.high_price_as_float(),second_ohlcv.low_price_as_float(),second_ohlcv.close_price_as_float())
                abs_relative_price_change = abs(second_price / first_price - 1)
                duration_seconds = second_ohlcv.start_unix_timestamp_seconds - first_ohlcv.start_unix_timestamp_seconds
                exchange.price_changes[symbol].append(abs_relative_price_change / math.sqrt(duration_seconds))

        exchange.ohlcvs.clear()

        async def track_price_changes():
            while True:
                if exchange.previous_bbos is not None:
                    for symbol in symbols:
                        abs_relative_price_change = abs(exchange.bbos[symbol].mid_price_as_float() / exchange.previous_bbos[symbol].mid_price_as_float() - 1)
                        exchange.price_changes[symbol].append(abs_relative_price_change)
                        if len(exchange.price_changes[symbol]) > price_change_look_back_seconds:
                            del exchange.price_changes[symbol][:-price_change_look_back_seconds]
                        if abs_relative_price_change > exchange.volatilities[symbol]:
                            exchange.volatilities[symbol] = abs_relative_price_change
                exchange.previous_bbos = exchange.bbos

                await asyncio.sleep(1)
        asyncio.create_task(coro=track_price_changes())

        async def calculate_statistics():
            while True:
                for symbol in symbols:
                    if exchange.price_changes[symbol]:
                        exchange.volatilities[symbol] = max(max(exchange.price_changes[symbol]), statistics.mean(exchange.price_changes[symbol]) * 3)
                await asyncio.sleep(60)
        asyncio.create_task(coro=calculate_statistics())

        small_sleep_seconds = float(os.getenv("SMALL_SLEEP_SECONDS", "0.05"))  # increase this value if connecting through a slow VPN
        refresh_interval_seconds = float(os.getenv("REFRESH_INTERVAL_SECONDS", "30"))
        refresh_interval_seconds_per_symbol = refresh_interval_seconds / len(symbols)
        min_volatility_multiplier = float(os.getenv("MIN_VOLATILITY_MULTIPLIER", "0.5"))
        max_volatility_multiplier = float(os.getenv("MAX_VOLATILITY_MULTIPLIER", "2"))
        max_num_open_orders_per_symbol_per_side = int(os.getenv("MAX_NUM_OPEN_ORDERS_PER_SYMBOL_PER_SIDE", "4"))
        order_quantity_safety_margin = 0.995

        while True:
            total_value = 0
            base_asset_quantities = {}
            base_asset_values = {}
            for symbol in symbols:
                price = exchange.bbos[symbol].mid_price_as_float
                base_asset = symbol_to_base_asset[symbol]
                base_asset_quantity = exchange.balances[base_asset].quantity_as_float if base_asset in exchange.balances else 0
                base_asset_quantities[symbol] = base_asset_quantity
                base_asset_value = price * base_asset_quantity
                base_asset_values[symbol] = base_asset_value
                total_value += base_asset_value
            quote_asset_quantity = exchange.balances[quote_asset].quantity_as_float if quote_asset in exchange.balances else 0
            total_value += quote_asset_quantity

            for symbol in symbols:
                if exchange.previous_bbos is None or exchange.bbos[symbol].best_bid_price_as_decimal() != previous_bbos[symbol].best_bid_price_as_decimal() or exchange.bbos[symbol].best_ask_price_as_decimal() != previous_bbos[symbol].best_ask_price_as_decimal():
                    logger.detail(f"[{symbol}] cancel orders")
                    await exchange.cancel_orders(symbol=symbol)
                    await asyncio.sleep(small_sleep_seconds)

                    instrument_information = exchange.all_instrument_information[symbol]
                    order_quantity_min_as_float = instrument_information.order_quantity_min_as_float or 0
                    order_quote_quantity_min_as_float = instrument_information.order_quote_quantity_min_as_float or 0
                    volatility = exchange.volatilities[symbol]
                    bbo = exchange.bbos[symbol]
                    base_asset_ratio = base_asset_values[symbol] / (total_value * base_asset_normalized_weights[symbol_to_base_asset[symbol]])
                    order_prices_as_decimal = {
                        True: set(), # buy
                        False: set(), # sell
                    }

                    for i in range(0, max_num_open_orders_per_symbol_per_side):
                        volatility_multiplier = ((max_volatility_multiplier - min_volatility_multiplier) / (max_num_open_orders_per_symbol_per_side - 1) * i + min_volatility_multiplier)

                        buy_volatility_multiplier = volatility_multiplier + base_asset_ratio * (max_volatility_multiplier - volatility_multiplier)
                        estimated_order_price_as_float = bbo.best_bid_price_as_float * (1 - buy_volatility_multiplier * volatility)
                        order_price_as_decimal = round_to_nearest(
                            input=estimated_order_price_as_float, increment=instrument_information.order_price_increment
                        )
                        order_prices_as_decimal[True].add(order_price_as_decimal)


                        sell_volatility_multiplier = volatility_multiplier + (1-base_asset_ratio) * (max_volatility_multiplier - volatility_multiplier)
                        estimated_order_price_as_float = bbo.best_ask_price_as_float * (1 + sell_volatility_multiplier * volatility)
                        order_price_as_decimal = round_to_nearest(
                            input=estimated_order_price_as_float, increment=instrument_information.order_price_increment
                        )
                        order_prices_as_decimal[False].add(order_price_as_decimal)

                    if order_prices_as_decimal[True]:
                        estimated_order_quote_quantity_as_float = (total_value * base_asset_normalized_weights[symbol_to_base_asset[symbol]] - base_asset_values[symbol])  / len(order_prices_as_decimal_for_side) * order_quantity_safety_margin
                        for order_price_as_decimal in order_prices_as_decimal[True]:
                            order_price_as_float = float(order_price_as_decimal)
                            order_quantity_as_decimal = round_down(
                                input=estimated_order_quote_quantity_as_float / order_price_as_float, increment=symbol_instrument_information.order_quantity_increment
                            )
                            order_quantity_as_float = float(order_quantity_as_decimal)
                            if order_quantity_as_float >= order_quantity_increment_as_float and order_price_as_float * order_quantity_as_float >= order_quote_quantity_min_as_float:
                                order_price_as_string = convert_decimal_to_string(
                                    input=order_price_as_decimal
                                )
                                order_quantity_as_string = convert_decimal_to_string(
                                    input=order_quantity_as_decimal
                                )
                                order = Order(
                                    symbol=symbol,
                                    is_buy=True,
                                    price=order_price_as_string,
                                    quantity=order_quantity_as_string,
                                    is_post_only=True,
                                )
                                logger.detail(f"[{symbol}] create buy order with price {order_price_as_string} and quantity {order_quantity_as_string}")
                                await exchange.create_order(order=order)
                                await asyncio.sleep(small_sleep_seconds)

                await asyncio.sleep(refresh_interval_seconds_per_symbol)

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    loop.create_task(main())
    loop.run_forever()
