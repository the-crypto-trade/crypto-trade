#!/usr/bin/env python3

# This example does not use any components of our library.
# It simply illustrates how straightforward it is to implement a fast backtest script from scratch.

import time
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import talib
from numba import njit


def timed_call(func, *args, **kwargs):
    wall_start = time.perf_counter()
    cpu_start = time.process_time()
    result = func(*args, **kwargs)
    wall_end = time.perf_counter()
    cpu_end = time.process_time()
    print(f"Wall-clock time: {wall_end - wall_start:.6f} seconds")
    print(f"CPU time:        {cpu_end - cpu_start:.6f} seconds")
    return result


@njit
def compute_order(price_i, rsi_i, total_value_i, cash, base_asset_size):
    # Strong buy (all-in) if RSI < 20
    if rsi_i < 20.0 and cash > 0.0:
        order_value = cash
        return 1, order_value

    # Partial buy if RSI < 30
    elif rsi_i < 30.0 and cash > 0.0:
        order_value = min(0.5 * total_value_i, cash)
        return 1, order_value

    # Strong sell (all-out) if RSI > 70
    elif rsi_i > 70.0 and base_asset_size > 0.0:
        order_value = base_asset_size * price_i
        return -1, order_value

    return 0, 0.0


@njit
def evaluate(price, rsi, initial_cash=1.0, initial_base_asset_size=0.0):
    n = len(price)
    total_value = np.empty(n)
    cash = initial_cash
    base_asset_size = initial_base_asset_size

    for i in range(n):
        price_i = price[i]
        rsi_i = rsi[i]
        total_value_i = cash + base_asset_size * price_i
        total_value[i] = total_value_i
        action, order_value = compute_order(price_i, rsi_i, total_value_i, cash, base_asset_size)
        if action == 1:  # Buy
            base_asset_size += order_value / price_i
            cash -= order_value
        elif action == -1:  # Sell
            base_asset_size -= order_value / price_i
            cash += order_value

    return total_value


def backtest(data):
    df = data.copy()
    price = df["price"].values
    rsi = df["rsi"].values
    total_value = evaluate(price, rsi)
    df["total_value"] = total_value
    return df


if __name__ == "__main__":
    data = pd.read_csv(
        f"{Path(__file__).parent}/sample_data/data.csv",
        parse_dates=["datetime"],
        index_col="datetime",
        usecols=["datetime", "close"],
    )
    data.index = data.index + pd.to_timedelta("1min")
    data.rename(columns={"close": "price"}, inplace=True)
    data["rsi"] = talib.RSI(data["price"].values, timeperiod=14)
    data.dropna(inplace=True)

    # Numba warm-up run (not timed)
    backtest(data)

    # Timed run
    df = timed_call(backtest, data)

    # Plot
    datetimes = df.index.to_list()
    prices = df["price"].tolist()
    total_values = df["total_value"].to_list()
    prices = [price / prices[0] * total_values[0] for price in prices]
    plt.plot(datetimes, prices, label="price", color="red")
    plt.plot(datetimes, total_values, label="account total value", color="green")
    plt.xlabel("Datetime")
    plt.ylabel("Value")
    plt.title("Price vs Account Total Value")
    plt.legend()
    plt.show()
