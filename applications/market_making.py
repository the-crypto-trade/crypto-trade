import argparse
import asyncio
import itertools
import math
import os
import statistics
import sys
import time
import traceback

from crypto_trade.exchange_api import Order
from crypto_trade.exchanges.bybit import Bybit, BybitInstrumentType
from crypto_trade.exchanges.okx import Okx, OkxInstrumentType
from crypto_trade.utility import (
    LoggerWithWriter,
    LogLevel,
    Writer,
    convert_decimal_to_string,
    round_down,
    round_to_nearest,
)

ORDER_QUANTITY_SAFETY_MARGIN = 0.995


def parse_base_assets(s):
    assets = []
    weights = {}
    has_weights = False

    for item in s.split(","):
        parts = item.strip().split(":")
        if len(parts) == 1:  # just the asset name
            asset = parts[0]
            assets.append(asset)
            weights[asset] = None
        elif len(parts) == 2:  # asset with weight
            asset, w = parts
            w = float(w)
            if w <= 0:
                raise argparse.ArgumentTypeError(f"Invalid weight for asset '{asset}': {w}. Must be positive.")
            assets.append(asset)
            weights[asset] = w
            has_weights = True
        else:
            raise argparse.ArgumentTypeError(f"Invalid base asset format: '{item}'. Use 'ASSET' or 'ASSET:WEIGHT'.")

    # assign equal weights if none provided
    if not has_weights:
        n = len(assets)
        weights = {asset: 1.0 / n for asset in assets}
    else:
        total = sum(weights.values())
        if total == 0:
            raise argparse.ArgumentTypeError("Base asset weights cannot all be zero.")
        weights = {k: v / total for k, v in weights.items()}

    return weights


async def main():
    try:
        # --- Environment variables ---
        is_paper_trading = os.getenv("IS_PAPER_TRADING", "false").lower() == "true"
        api_key = os.getenv("API_KEY", "")
        api_secret = os.getenv("API_SECRET", "")
        api_passphrase = os.getenv("API_PASSPHRASE", "")
        log_dir = os.getenv("LOG_DIR", "")

        # --- Command line arguments ---
        parser = argparse.ArgumentParser()
        parser.add_argument("--exchange-name", required=True, type=str.lower, help="Name of the exchange (e.g. bybit).")
        parser.add_argument(
            "--base-assets",
            required=True,
            type=parse_base_assets,
            help="Base assets, either e.g. 'BTC,ETH,SOL' (equal capital weights) or e.g. 'BTC:0.5,ETH:0.3,SOL:0.2' (custom capital weights).",
        )
        parser.add_argument("--quote-asset", required=True, type=str.upper, help="Quote asset (e.g. USDT)")
        parser.add_argument(
            "--price-change-look-back-seconds", type=int, default=18000, help="Seconds to look back for price changes. Used to calculate market statistics."
        )
        parser.add_argument("--refresh-interval-seconds", type=float, default=30, help="Interval between order refreshes.")
        parser.add_argument(
            "--min-volatility-multiplier",
            type=float,
            default=0.5,
            help="Minimum volatility multiplier. Can be any non-negative number. Used to calculate quote price levels.",
        )
        parser.add_argument(
            "--max-volatility-multiplier",
            type=float,
            default=1.5,
            help="Maximum volatility multiplier. Can be any non-negative number. Used to calculate quote price levels.",
        )
        parser.add_argument("--max-num-open-orders-per-symbol-per-side", type=int, default=4, help="Max open orders per symbol per side.")
        parser.add_argument(
            "--start-wait-seconds", type=float, default=1, help="Initial delay before trading. Increase this value if connecting through a slow VPN."
        )
        parser.add_argument("--send-consecutive-create-order-request-delay-seconds", type=float, default=0.05, help="Due to rate limit.")
        parser.add_argument("--send-consecutive-cancel-order-request-delay-seconds", type=float, default=0.05, help="Due to rate limit.")
        parser.add_argument(
            "--log-level",
            type=str.lower,
            default="info",
            choices=["trace", "debug", "fine", "detail", "info", "warning", "error", "critical", "none"],
            help="Logging level.",
        )
        args = parser.parse_args()

        exchange_name = args.exchange_name

        quote_asset = args.quote_asset
        price_change_look_back_seconds = args.price_change_look_back_seconds
        refresh_interval_seconds = args.refresh_interval_seconds
        min_volatility_multiplier = args.min_volatility_multiplier
        max_volatility_multiplier = args.max_volatility_multiplier
        max_num_open_orders_per_symbol_per_side = args.max_num_open_orders_per_symbol_per_side
        start_wait_seconds = args.start_wait_seconds
        send_consecutive_create_order_request_delay_seconds = args.send_consecutive_create_order_request_delay_seconds
        send_consecutive_cancel_order_request_delay_seconds = args.send_consecutive_cancel_order_request_delay_seconds
        log_level = args.log_level

        # --- Logger setup ---
        write_dir = os.path.join(log_dir, os.path.splitext(os.path.basename(__file__))[0]) if log_dir else None
        writer = Writer(end="\n", write_dir=write_dir, write_buffering=1)
        logger = LoggerWithWriter(level=getattr(LogLevel, log_level.upper()), name="", writer=writer)

        # --- Base asset weights ---
        base_asset_weights = args.base_assets

        # --- Symbols & instrument type ---
        symbols = []
        symbol_to_base_asset = {}
        instrument_type_cls = {"bybit": BybitInstrumentType, "okx": OkxInstrumentType}[exchange_name]
        instrument_type = instrument_type_cls.SPOT
        exchange_class = {"bybit": Bybit, "okx": Okx}[exchange_name]

        for base_asset in base_asset_weights.keys():
            symbol = f"{base_asset}{quote_asset}" if exchange_name == "bybit" else f"{base_asset}-{quote_asset}"
            symbols.append(symbol)
            symbol_to_base_asset[symbol] = base_asset

        unix_timestamp_seconds = int(time.time())

        # --- Derived Exchange ---
        class DerivedExchange(exchange_class):
            def __init__(self, **kwargs):
                super().__init__(**kwargs)
                self.price_changes = {symbol: [] for symbol in symbols}
                self.previous_bbos = None
                self.volatilities = {symbol: 0.05 for symbol in symbols}

        exchange = DerivedExchange(
            instrument_type=instrument_type,
            symbols=symbols,
            subscribe_bbo=True,
            ohlcv_interval_seconds=60,
            fetch_historical_ohlcv_at_start=True,
            fetch_historical_ohlcv_start_unix_timestamp_seconds=unix_timestamp_seconds - price_change_look_back_seconds,
            fetch_historical_ohlcv_end_unix_timestamp_seconds=unix_timestamp_seconds,
            subscribe_order=True,
            subscribe_fill=True,
            subscribe_balance=True,
            rest_account_cancel_open_order_at_start=True,
            is_paper_trading=is_paper_trading,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            logger=logger,
            start_wait_seconds=start_wait_seconds,
            send_consecutive_cancel_order_request_delay_seconds=send_consecutive_cancel_order_request_delay_seconds,
        )

        await exchange.start()

        for symbol, ohlcvs_for_symbol in exchange.ohlcvs.items():
            for pairwise_ohlcvs_for_symbol in itertools.pairwise(ohlcvs_for_symbol):
                first_ohlcv = pairwise_ohlcvs_for_symbol[0]
                second_ohlcv = pairwise_ohlcvs_for_symbol[1]
                first_price = statistics.geometric_mean(
                    [first_ohlcv.open_price_as_float, first_ohlcv.high_price_as_float, first_ohlcv.low_price_as_float, first_ohlcv.close_price_as_float]
                )
                second_price = statistics.geometric_mean(
                    [second_ohlcv.open_price_as_float, second_ohlcv.high_price_as_float, second_ohlcv.low_price_as_float, second_ohlcv.close_price_as_float]
                )
                abs_relative_price_change = abs(second_price / first_price - 1)
                duration_seconds = second_ohlcv.start_unix_timestamp_seconds - first_ohlcv.start_unix_timestamp_seconds
                exchange.price_changes[symbol].append(abs_relative_price_change / math.sqrt(duration_seconds))

        exchange.ohlcvs.clear()

        # --- Helper: create orders ---
        async def create_orders(
            symbol,
            is_buy,
            order_prices,
            estimated_order_quantity,
            estimated_order_quote_quantity,
            order_quantity_increment_as_float,
            order_quote_quantity_min_as_float,
            symbol_instrument_information,
        ):
            for order_price_as_decimal in sorted(order_prices, reverse=True) if is_buy else sorted(order_prices):
                order_price_as_float = float(order_price_as_decimal)
                qty_input = (estimated_order_quote_quantity / order_price_as_float if is_buy else estimated_order_quantity) * ORDER_QUANTITY_SAFETY_MARGIN
                order_quantity_as_decimal = round_down(input=qty_input, increment=symbol_instrument_information.order_quantity_increment)
                order_quantity_as_float = float(order_quantity_as_decimal)

                if (
                    order_quantity_as_float >= order_quantity_increment_as_float
                    and order_price_as_float * order_quantity_as_float >= order_quote_quantity_min_as_float
                ):
                    order_price_str = convert_decimal_to_string(input=order_price_as_decimal)
                    order_quantity_str = convert_decimal_to_string(input=order_quantity_as_decimal)
                    order = Order(symbol=symbol, is_buy=is_buy, price=order_price_str, quantity=order_quantity_str, is_post_only=True)
                    side_text = "buy" if is_buy else "sell"
                    logger.info(f"[{symbol}] create {side_text} order with price {order_price_str} and quantity {order_quantity_str}")
                    await exchange.create_order(order=order)
                    await asyncio.sleep(send_consecutive_create_order_request_delay_seconds)

        # --- Track price changes ---
        async def track_price_changes():
            try:
                while True:
                    if exchange.previous_bbos:
                        for symbol in symbols:
                            prev = exchange.previous_bbos[symbol].mid_price_as_float
                            curr = exchange.bbos[symbol].mid_price_as_float
                            abs_rel_change = abs(curr / prev - 1)
                            exchange.price_changes[symbol].append(abs_rel_change)
                            if len(exchange.price_changes[symbol]) > price_change_look_back_seconds:
                                del exchange.price_changes[symbol][:-price_change_look_back_seconds]
                            if abs_rel_change > exchange.volatilities[symbol]:
                                exchange.volatilities[symbol] = abs_rel_change
                    exchange.previous_bbos = exchange.bbos.copy()
                    await asyncio.sleep(1)
            except Exception as exception:
                logger.error(exception)

        # --- Calculate volatilities periodically ---
        async def calculate_statistics():
            try:
                while True:
                    for symbol in symbols:
                        changes = exchange.price_changes[symbol]
                        if changes:
                            volatility = max(max(changes), statistics.mean(changes) * 3)
                            if volatility > 0:
                                exchange.volatilities[symbol] = volatility
                    await asyncio.sleep(60)
            except Exception as exception:
                logger.error(exception)

        asyncio.create_task(track_price_changes())
        asyncio.create_task(calculate_statistics())

        await asyncio.sleep(1)
        # --- Main trading loop ---
        while True:
            total_value = 0

            # Compute total values
            for symbol in symbols:
                price = exchange.bbos[symbol].mid_price_as_float
                base_asset = symbol_to_base_asset[symbol]
                base_asset_quantity = exchange.balances[base_asset].quantity_as_float if base_asset in exchange.balances else 0
                base_asset_value = price * base_asset_quantity
                total_value += base_asset_value

            quote_asset_quantity = exchange.balances[quote_asset].quantity_as_float if quote_asset in exchange.balances else 0
            total_value += quote_asset_quantity

            if total_value <= 0:
                raise ValueError(f"Computed total_value must be positive, got {total_value}")

            logger.info(f"total_value = {total_value}")

            first_time_symbols = symbols.copy()

            # Process each symbol
            for symbol in symbols:
                if symbol in first_time_symbols or (
                    (
                        exchange.bbos[symbol].best_bid_price != exchange.previous_bbos[symbol].best_bid_price
                        or exchange.bbos[symbol].best_ask_price != exchange.previous_bbos[symbol].best_ask_price
                    )
                    and exchange.bbos[symbol].best_bid_price
                    and exchange.bbos[symbol].best_ask_price
                ):
                    first_time_symbols.remove(symbol)
                    logger.info(f"[{symbol}] cancel orders")
                    await exchange.cancel_orders(symbol=symbol)

                    info = exchange.all_instrument_information[symbol]
                    order_quote_min = info.order_quote_quantity_min_as_float or 0
                    volatility = exchange.volatilities[symbol] * math.sqrt(refresh_interval_seconds)
                    logger.info(f"[{symbol}] volatility = {volatility}")
                    bbo = exchange.bbos[symbol]
                    logger.info(f"[{symbol}] bbo.best_bid_price = {bbo.best_bid_price}, bbo.best_ask_price = {bbo.best_ask_price}")

                    price = bbo.mid_price_as_float
                    base_asset = symbol_to_base_asset[symbol]
                    base_asset_quantity = exchange.balances[base_asset].quantity_as_float if base_asset in exchange.balances else 0
                    base_asset_value = price * base_asset_quantity
                    quote_asset_quantity = exchange.balances[quote_asset].quantity_as_float if quote_asset in exchange.balances else 0
                    logger.info(
                        f"[{symbol}] base_asset_quantity = {base_asset_quantity}, base_asset_value = {base_asset_value}, quote_asset_quantity = {quote_asset_quantity}"
                    )

                    available_quote_asset_quantity = quote_asset_quantity
                    for orders_for_a_symbol in exchange.orders.values():
                        for order in orders_for_a_symbol:
                            if not order.is_closed and order.is_buy:
                                available_quote_asset_quantity -= order.price_as_float * order.quantity_as_float
                    logger.info(f"[{symbol}] available_quote_asset_quantity = {available_quote_asset_quantity}")

                    target_base_asset_value = total_value * base_asset_weights[symbol_to_base_asset[symbol]] * 0.5
                    base_ratio = (base_asset_value - target_base_asset_value) / target_base_asset_value
                    order_prices_as_decimal = {True: set(), False: set()}

                    for i in range(max_num_open_orders_per_symbol_per_side):
                        volatility_multiplier = (max_volatility_multiplier - min_volatility_multiplier) / (
                            max_num_open_orders_per_symbol_per_side - 1
                        ) * i + min_volatility_multiplier

                        # Buy price
                        buy_volatility_multiplier = max(
                            (
                                volatility_multiplier + base_ratio * (max_volatility_multiplier - volatility_multiplier)
                                if base_ratio >= 0
                                else volatility_multiplier + base_ratio * (volatility_multiplier - min_volatility_multiplier)
                            ),
                            0,
                        )
                        buy_price = round_to_nearest(
                            input=bbo.best_bid_price_as_float * (1 - buy_volatility_multiplier * volatility), increment=info.order_price_increment
                        )
                        order_prices_as_decimal[True].add(buy_price)

                        # Sell price
                        sell_volatility_multiplier = max(
                            (
                                volatility_multiplier - base_ratio * (volatility_multiplier - min_volatility_multiplier)
                                if base_ratio >= 0
                                else volatility_multiplier - base_ratio * (max_volatility_multiplier - volatility_multiplier)
                            ),
                            0,
                        )
                        sell_price = round_to_nearest(
                            input=bbo.best_ask_price_as_float * (1 + sell_volatility_multiplier * volatility), increment=info.order_price_increment
                        )
                        order_prices_as_decimal[False].add(sell_price)

                    # Create buy orders
                    if order_prices_as_decimal[True]:
                        estimated_buy_quote_qty = min(
                            total_value * base_asset_weights[symbol_to_base_asset[symbol]] - base_asset_value, available_quote_asset_quantity
                        ) / len(order_prices_as_decimal[True])
                        await create_orders(
                            symbol,
                            True,
                            order_prices_as_decimal[True],
                            None,
                            estimated_buy_quote_qty,
                            info.order_quantity_min_as_float,
                            order_quote_min,
                            info,
                        )

                    # Create sell orders
                    if order_prices_as_decimal[False]:
                        estimated_sell_qty = base_asset_quantity / len(order_prices_as_decimal[False])
                        await create_orders(
                            symbol, False, order_prices_as_decimal[False], estimated_sell_qty, None, info.order_quantity_min_as_float, order_quote_min, info
                        )
                sleep_time_in_seconds = refresh_interval_seconds / len(symbols)
                logger.info(f"about to sleep for {sleep_time_in_seconds} seconds")
                await asyncio.sleep(sleep_time_in_seconds)

    except Exception:
        print(traceback.format_exc())
        sys.exit("exit")


if __name__ == "__main__":
    asyncio.run(main())
