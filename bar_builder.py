#!/usr/bin/env python3
"""
bar_builder.py

1) Read raw tick data (T&S) from ArcticDB (assumes data is stored under "symbol_tas").
2) Convert Sierra Chart microsecond timestamps -> normal datetimes.
3) Build time-based, trade-based, and volume-based OHLCV bars.
4) Store each bar set back into ArcticDB under e.g. "symbol_bars_1min", etc.
"""

import pandas as pd
import numpy as np
from arcticdb import Arctic
from json import loads

############
# CONFIG
############

ARCTIC_HOST = "mongodb://localhost:27017"
LIB_NAME    = "tick_data"

# Read your local config.json to get the UTC offset, etc.
CONFIG = loads(open("./config.json").read())
UTC_OFFSET_US = int(CONFIG["utc_offset"] * 3.6e9)  # e.g. offset in microseconds

############
# ARCTIC
############

store = Arctic(ARCTIC_HOST)
arctic_lib = store[LIB_NAME]

############
# FUNCTIONS
############

def get_tick_data(symbol: str) -> pd.DataFrame:
    """
    Load tick data (T&S) from ArcticDB under symbol + '_tas'.
    Then convert Sierra Chart timestamps into a normal DateTime index.
    Returns a DataFrame with columns: [timestamp, price, qty, side, ...] + DatetimeIndex.
    """
    lib_symbol = f"{symbol}_tas"
    df = arctic_lib.read(lib_symbol).data

    # Convert Sierra Chart microsecond timestamps -> normal datetimes
    # SC epoch is 1899-12-30, plus your local UTC offset, plus the microseconds in 'timestamp'.
    df["datetime"] = (
        np.datetime64("1899-12-30")
        + np.timedelta64(UTC_OFFSET_US, "us")
        + df["timestamp"].values.astype("timedelta64[us]")
    ).astype("datetime64[ns]")  # or datetime64[us] if you prefer microsecond resolution

    # Set 'datetime' as the DateTime index
    df.set_index("datetime", inplace=True, drop=True)

    return df


def build_time_bars(df: pd.DataFrame, freq: str = '1Min') -> pd.DataFrame:
    """
    Resample T&S data into time-based bars at frequency `freq`.
    freq examples: '1Min', '5Min', '1H', etc.

    For each bar:
      - open: first trade price
      - high: max trade price
      - low:  min trade price
      - close: last trade price
      - volume: sum of all quantities in that window
    """
    # Ensure DF has a DatetimeIndex for resampling
    ohlcv = df.resample(freq).agg({
        'price': ['first', 'max', 'min', 'last'],
        'qty': 'sum'
    })
    ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']
    # Drop bars that have no trades
    ohlcv.dropna(subset=['open'], inplace=True)
    return ohlcv


def build_trade_bars(df: pd.DataFrame, trades_per_bar: int = 100) -> pd.DataFrame:
    """
    Build bars every N trades. We'll accumulate trades until we hit `trades_per_bar`.
    Then output an OHLCV bar and start again.

    For each bar:
      - open: price of first trade in the group
      - high: max price in the group
      - low:  min price in the group
      - close: price of last trade
      - volume: sum of all qty in the group
    """
    # Sort by timestamp in case it's not sorted
    df = df.sort_values('timestamp').reset_index(drop=True)

    # Create a 'bar_id' by integer division of row index
    df['bar_id'] = df.index // trades_per_bar

    grouped = df.groupby('bar_id')
    ohlcv = grouped.agg(
        open=('price', 'first'),
        high=('price', 'max'),
        low=('price', 'min'),
        close=('price', 'last'),
        volume=('qty', 'sum'),
        bar_time_first=('timestamp', 'first'),
        bar_time_last=('timestamp', 'last'),
    )

    # Optional: set the final bar timestamp to bar_time_last, etc.
    ohlcv.index.name = None
    ohlcv.reset_index(drop=True, inplace=True)
    return ohlcv


def build_volume_bars(df: pd.DataFrame, volume_per_bar: int = 1000) -> pd.DataFrame:
    """
    Build bars every N total volume. Accumulate trades until total qty >= volume_per_bar.
    Then start a new bar.

    For each bar:
      - open: first trade price in that bar
      - high: max trade price
      - low:  min trade price
      - close: last trade price
      - volume: sum of traded qty in that bar
    """
    df = df.sort_values('timestamp').reset_index(drop=True)

    bars = []
    bar_open = bar_high = bar_low = bar_close = None
    bar_volume = 0
    bar_start_ts = bar_end_ts = None

    for idx, row in df.iterrows():
        price = row['price']
        qty   = row['qty']
        tstamp = row['timestamp']

        # Start a new bar if needed
        if bar_open is None:
            bar_open = bar_high = bar_low = bar_close = price
            bar_start_ts = tstamp
            bar_volume = 0

        # Update bar stats
        bar_close = price
        bar_high = max(bar_high, price)
        bar_low = min(bar_low, price)
        bar_volume += qty
        bar_end_ts = tstamp

        # If we hit or exceed the volume threshold, close out this bar
        if bar_volume >= volume_per_bar:
            bars.append({
                'open': bar_open,
                'high': bar_high,
                'low': bar_low,
                'close': bar_close,
                'volume': bar_volume,
                'bar_time_first': bar_start_ts,
                'bar_time_last': bar_end_ts,
            })
            # Reset for next bar
            bar_open = bar_high = bar_low = bar_close = None
            bar_volume = 0

    # Convert to DataFrame
    ohlcv = pd.DataFrame(bars)
    return ohlcv


def store_bars_arctic(symbol: str, df_bars: pd.DataFrame, suffix: str):
    """
    Store OHLCV bars back to Arctic under a new symbol name
    (e.g. "symbol_bars_suffix").
    """
    lib_symbol = f"{symbol}_bars_{suffix}"
    arctic_lib.write(lib_symbol, df_bars)
    print(f"Stored bar data to Arctic: {lib_symbol}")


############
# MAIN
############

if __name__ == "__main__":
    # Example usage
    symbol = "NQH25_FUT_CME"

    # 1) Load T&S data and convert microsecond timestamps to normal datetime index
    df_ticks = get_tick_data(symbol)

    # 2) Build 1-minute time bars, trade-based bars, and volume-based bars
    df_timebars = build_time_bars(df_ticks, freq='1Min')
    df_tradebars = build_trade_bars(df_ticks, trades_per_bar=100)
    df_volbars = build_volume_bars(df_ticks, volume_per_bar=1000)

    # 3) Store each bar set back to Arctic, under separate suffixes
    store_bars_arctic(symbol, df_timebars, suffix='1min')
    store_bars_arctic(symbol, df_tradebars, suffix='trade100')
    store_bars_arctic(symbol, df_volbars, suffix='vol1000')

    print("Done generating and storing sample bars.")
