import argparse
import asyncio
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
            "--base-assets", required=True, type=lambda s: [x.strip() for x in s.split(",")], help="Comma-separated list of base assets (e.g. BTC,ETH,SOL)."
        )
        parser.add_argument("--quote-asset", required=True, type=str.upper, help="Quote asset (e.g. USDT)")
        parser.add_argument(
            "--base-asset-weights",
            type=lambda s: {k: float(v) for x in s.split(",") if (parts := x.strip().split(":")) and len(parts) == 2 for k, v in [parts]},
            help="Optional weights for base assets, e.g. 'BTC:0.5,ETH:0.3,SOL:0.2'. If omitted, equal weights are assigned and normalized.",
        )
        parser.add_argument("--price-change-look-back-seconds", type=int, default=18000, help="Seconds to look back for price changes.")
        parser.add_argument("--refresh-interval-seconds", type=float, default=30, help="Interval between order refreshes.")
        parser.add_argument("--min-volatility-multiplier", type=float, default=0.5, help="Minimum volatility multiplier.")
        parser.add_argument("--max-volatility-multiplier", type=float, default=2.0, help="Maximum volatility multiplier.")
        parser.add_argument("--max-num-open-orders-per-symbol-per-side", type=int, default=4, help="Max open orders per symbol per side.")
        parser.add_argument("--start-wait-seconds", type=float, default=1, help="Initial delay before trading.")
        parser.add_argument("--small-sleep-seconds", type=float, default=0.05, help="Sleep interval for tight loops.")
        parser.add_argument(
            "--log-level",
            type=str.lower,
            default="warning",
            choices=["trace", "debug", "fine", "detail", "info", "warning", "error", "critical", "none"],
            help="Logging level.",
        )
        args = parser.parse_args()

        exchange_name = args.exchange_name
        base_assets = args.base_assets
        quote_asset = args.quote_asset
        price_change_look_back_seconds = args.price_change_look_back_seconds
        refresh_interval_seconds = args.refresh_interval_seconds
        min_volatility_multiplier = args.min_volatility_multiplier
        max_volatility_multiplier = args.max_volatility_multiplier
        max_num_open_orders_per_symbol_per_side = args.max_num_open_orders_per_symbol_per_side
        start_wait_seconds = args.start_wait_seconds
        small_sleep_seconds = args.small_sleep_seconds
        log_level = args.log_level

        # --- Logger setup ---
        write_dir = os.path.join(log_dir, os.path.splitext(os.path.basename(__file__))[0])
        writer = Writer(end="\n", write_dir=write_dir, write_buffering=1)
        logger = LoggerWithWriter(level=getattr(LogLevel, log_level.upper()), name="", writer=writer)

        # --- Base asset weights ---
        base_asset_weights = args.base_asset_weights
        if base_asset_weights is None:
            n = len(base_assets)
            base_asset_weights = {asset: 1.0 / n for asset in base_assets}
        else:
            total = sum(base_asset_weights.values())
            if total == 0:
                raise ValueError("Base asset weights cannot all be zero.")
            base_asset_weights = {k: v / total for k, v in base_asset_weights.items()}

        # --- Symbols & instrument type ---
        symbols = []
        symbol_to_base_asset = {}
        instrument_type_cls = {"bybit": BybitInstrumentType, "okx": OkxInstrumentType}[exchange_name]
        instrument_type = instrument_type_cls.SPOT
        exchange_class = {"bybit": Bybit, "okx": Okx}[exchange_name]

        for base_asset in base_assets:
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

            async def update_websocket_push_data_for_fill(self, *, fills):
                if fills:
                    self.logger.info("fills", fills)
                    for fill in fills:
                        await self.cancel_orders(symbol=fill.symbol)

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
            is_paper_trading=is_paper_trading,
            api_key=api_key,
            api_secret=api_secret,
            api_passphrase=api_passphrase,
            logger=logger,
            start_wait_seconds=start_wait_seconds,
        )

        await exchange.start()

        # --- Helper: create orders ---
        async def create_orders(
            symbol,
            is_buy,
            order_prices,
            estimated_order_quantity,
            order_quantity_increment_as_float,
            order_quote_quantity_min_as_float,
            symbol_instrument_information,
        ):
            for order_price_as_decimal in order_prices:
                order_price_as_float = float(order_price_as_decimal)
                qty_input = estimated_order_quantity / order_price_as_float if is_buy else estimated_order_quantity
                order_quantity_as_decimal = round_down(input=qty_input, increment=symbol_instrument_information.order_quantity_increment)
                order_quantity_as_float = float(order_quantity_as_decimal)

                if (
                    order_quantity_as_float >= order_quantity_increment_as_float
                    and order_price_as_float * order_quantity_as_float >= order_quote_quantity_min_as_float
                ):
                    order_price_str = convert_decimal_to_string(order_price_as_decimal)
                    order_quantity_str = convert_decimal_to_string(order_quantity_as_decimal)
                    order = Order(symbol=symbol, is_buy=is_buy, price=order_price_str, quantity=order_quantity_str, is_post_only=True)
                    side_text = "buy" if is_buy else "sell"
                    logger.detail(f"[{symbol}] create {side_text} order with price {order_price_str} and quantity {order_quantity_str}")
                    await exchange.create_order(order=order)
                    await asyncio.sleep(small_sleep_seconds)

        # --- Track price changes ---
        async def track_price_changes():
            while True:
                if exchange.previous_bbos:
                    for symbol in symbols:
                        prev = exchange.previous_bbos[symbol].mid_price_as_float()
                        curr = exchange.bbos[symbol].mid_price_as_float()
                        abs_rel_change = abs(curr / prev - 1)
                        exchange.price_changes[symbol].append(abs_rel_change)
                        if len(exchange.price_changes[symbol]) > price_change_look_back_seconds:
                            del exchange.price_changes[symbol][:-price_change_look_back_seconds]
                        if abs_rel_change > exchange.volatilities[symbol]:
                            exchange.volatilities[symbol] = abs_rel_change
                exchange.previous_bbos = exchange.bbos
                await asyncio.sleep(1)

        # --- Calculate volatilities periodically ---
        async def calculate_statistics():
            while True:
                for symbol in symbols:
                    changes = exchange.price_changes[symbol]
                    if changes:
                        exchange.volatilities[symbol] = max(max(changes), statistics.mean(changes) * 3)
                await asyncio.sleep(60)

        asyncio.create_task(track_price_changes())
        asyncio.create_task(calculate_statistics())

        # --- Main trading loop ---
        while True:
            total_value = 0
            base_asset_quantities = {}
            base_asset_values = {}

            # Compute total values
            for symbol in symbols:
                price = exchange.bbos[symbol].mid_price_as_float()
                base_asset = symbol_to_base_asset[symbol]
                qty = exchange.balances[base_asset].quantity_as_float if base_asset in exchange.balances else 0
                base_asset_quantities[symbol] = qty
                val = price * qty
                base_asset_values[symbol] = val
                total_value += val

            quote_qty = exchange.balances[quote_asset].quantity_as_float if quote_asset in exchange.balances else 0
            total_value += quote_qty

            # Process each symbol
            for symbol in symbols:
                if (
                    exchange.previous_bbos is None
                    or exchange.bbos[symbol].best_bid_price_as_decimal() != exchange.previous_bbos[symbol].best_bid_price_as_decimal()
                    or exchange.bbos[symbol].best_ask_price_as_decimal() != exchange.previous_bbos[symbol].best_ask_price_as_decimal()
                ):
                    await exchange.cancel_orders(symbol=symbol)
                    await asyncio.sleep(small_sleep_seconds)

                    info = exchange.all_instrument_information[symbol]
                    order_quote_min = info.order_quote_quantity_min_as_float or 0
                    volatility = exchange.volatilities[symbol]
                    bbo = exchange.bbos[symbol]

                    base_ratio_denom = total_value * base_asset_weights[symbol_to_base_asset[symbol]]
                    base_ratio = base_asset_values[symbol] / base_ratio_denom if base_ratio_denom else 0

                    order_prices_as_decimal = {True: set(), False: set()}

                    for i in range(max_num_open_orders_per_symbol_per_side):
                        vm = (max_volatility_multiplier - min_volatility_multiplier) / (
                            max_num_open_orders_per_symbol_per_side - 1
                        ) * i + min_volatility_multiplier

                        # Buy price
                        buy_vm = vm + base_ratio * (max_volatility_multiplier - vm)
                        buy_price = round_to_nearest(input=bbo.best_bid_price_as_float() * (1 - buy_vm * volatility), increment=info.order_price_increment)
                        order_prices_as_decimal[True].add(buy_price)

                        # Sell price
                        sell_vm = vm + (1 - base_ratio) * (max_volatility_multiplier - vm)
                        sell_price = round_to_nearest(input=bbo.best_ask_price_as_float() * (1 + sell_vm * volatility), increment=info.order_price_increment)
                        order_prices_as_decimal[False].add(sell_price)

                    # Create buy orders
                    if order_prices_as_decimal[True]:
                        estimated_buy_qty = (
                            (total_value * base_asset_weights[symbol_to_base_asset[symbol]] - base_asset_values[symbol])
                            / len(order_prices_as_decimal[True])
                            * ORDER_QUANTITY_SAFETY_MARGIN
                        )
                        await create_orders(
                            symbol, True, order_prices_as_decimal[True], estimated_buy_qty, info.order_quantity_min_as_float, order_quote_min, info
                        )

                    # Create sell orders
                    if order_prices_as_decimal[False]:
                        estimated_sell_qty = base_asset_quantities[symbol] / len(order_prices_as_decimal[False]) * ORDER_QUANTITY_SAFETY_MARGIN
                        await create_orders(
                            symbol, False, order_prices_as_decimal[False], estimated_sell_qty, info.order_quantity_min_as_float, order_quote_min, info
                        )

                await asyncio.sleep(refresh_interval_seconds / len(symbols))

    except Exception:
        print(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
