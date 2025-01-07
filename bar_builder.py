#!/usr/bin/env python3
"""
bar_builder.py - Optimized version

1) Read raw tick data (T&S) from ArcticDB (assumes data is stored under "symbol_tas").
2) Convert Sierra Chart microsecond timestamps -> Central time datetimes.
3) Build time-based, trade-based, and volume-based OHLCV bars.
4) Store each bar set back into ArcticDB under e.g. "symbol_bars_1min", etc.
"""

import pandas as pd
import numpy as np
from arcticdb import Arctic
from json import loads
from datetime import time
import logging
from logging.handlers import RotatingFileHandler
import os

############
# LOGGING
############

def setup_logging(log_dir="logs"):
    """Configure logging with both file and console handlers."""
    if not os.path.exists(log_dir):
        os.makedirs(log_dir)

    logger = logging.getLogger('bar_builder')
    logger.setLevel(logging.INFO)

    file_formatter = logging.Formatter(
        '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    console_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - %(message)s'
    )

    file_handler = RotatingFileHandler(
        os.path.join(log_dir, 'bar_builder.log'),
        maxBytes=10*1024*1024,
        backupCount=5
    )
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(file_formatter)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(console_formatter)

    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    return logger

logger = setup_logging()

############
# CONFIG
############

ARCTIC_HOST = "mongodb://localhost:27017"
LIB_NAME    = "tick_data"

try:
    CONFIG = loads(open("./config.json").read())
    UTC_OFFSET_US = int(CONFIG["utc_offset"] * 3.6e9)
    SESSION_START = time.fromisoformat(CONFIG.get("session_start", "08:30:00"))
    SESSION_END = time.fromisoformat(CONFIG.get("session_end", "14:59:59"))
    NEW_BAR_AT_SESSION_START = CONFIG.get("new_bar_at_session_start", True)
except Exception as e:
    logger.error(f"Failed to load config: {str(e)}")
    raise

############
# ARCTIC
############

try:
    store = Arctic(ARCTIC_HOST)
    arctic_lib = store[LIB_NAME]
    logger.info("Successfully connected to ArcticDB")
except Exception as e:
    logger.error(f"Failed to connect to ArcticDB: {str(e)}")
    raise

############
# FUNCTIONS
############

def get_tick_data(symbol: str) -> pd.DataFrame:
    """Optimized tick data loading with vectorized timestamp conversion."""
    lib_symbol = f"{symbol}_tas"
    logger.info(f"Loading tick data for {lib_symbol}")
    
    try:
        df = arctic_lib.read(lib_symbol).data
        
        # Vectorized timestamp conversion
        df["datetime"] = (
            np.datetime64("1899-12-30")
            + np.timedelta64(UTC_OFFSET_US, "us")
            + df["timestamp"].astype("timedelta64[us]")
        ).astype("datetime64[ns]")

        df.set_index("datetime", inplace=True, drop=True)
        
        logger.info(f"Successfully loaded {len(df)} ticks for {symbol}")
        return df
        
    except Exception as e:
        logger.error(f"Failed to load tick data for {lib_symbol}: {str(e)}")
        raise

def filter_session_hours(df: pd.DataFrame) -> pd.DataFrame:
    """Vectorized session hours filtering."""
    mask = (
        (df.index.time >= SESSION_START) &
        (df.index.time <= SESSION_END)
    )
    filtered_df = df[mask]
    logger.debug(f"Filtered {len(df)} rows to {len(filtered_df)} rows within session hours")
    return filtered_df

def build_time_bars(df: pd.DataFrame, freq: str = '1Min') -> pd.DataFrame:
    """Optimized time-based bar building using pure pandas operations."""
    logger.info(f"Building {freq} time bars")
    
    df = filter_session_hours(df)
    
    if NEW_BAR_AT_SESSION_START:
        # Handle session boundaries efficiently using groupby
        grouped = df.groupby(pd.Grouper(freq=freq, offset=pd.Timedelta(
            hours=SESSION_START.hour,
            minutes=SESSION_START.minute,
            seconds=SESSION_START.second
        )))
    else:
        grouped = df.groupby(pd.Grouper(freq=freq))
    
    # Vectorized aggregation
    ohlcv = grouped.agg({
        'price': ['first', 'max', 'min', 'last'],
        'qty': 'sum'
    })
    
    ohlcv.columns = ['open', 'high', 'low', 'close', 'volume']
    ohlcv.dropna(subset=['open'], inplace=True)
    
    logger.info(f"Generated {len(ohlcv)} {freq} bars")
    return ohlcv

def build_trade_bars(df: pd.DataFrame, trades_per_bar: int = 100) -> pd.DataFrame:
    """Optimized trade-based bar building using numpy operations."""
    logger.info(f"Building trade bars with {trades_per_bar} trades per bar")
    
    df = filter_session_hours(df)
    df = df.sort_index()
    
    # Create bar IDs using integer division
    n_trades = len(df)
    base_bar_ids = np.arange(n_trades) // trades_per_bar
    
    if NEW_BAR_AT_SESSION_START:
        # Find session starts
        session_starts = df.index.time == SESSION_START
        # Increment bar IDs after session starts
        bar_ids = base_bar_ids + np.cumsum(session_starts)
    else:
        bar_ids = base_bar_ids
    
    # Group and aggregate
    grouped = df.groupby(bar_ids)
    prices = grouped['price']
    volumes = grouped['qty']
    times = grouped.apply(lambda x: pd.Series({
        'bar_time_first': x.index[0],
        'bar_time_last': x.index[-1]
    }))
    
    # Combine results
    ohlcv = pd.DataFrame({
        'open': prices.first(),
        'high': prices.max(),
        'low': prices.min(),
        'close': prices.last(),
        'volume': volumes.sum(),
        'bar_time_first': times['bar_time_first'],
        'bar_time_last': times['bar_time_last']
    })
    
    logger.info(f"Generated {len(ohlcv)} trade bars")
    return ohlcv

def build_volume_bars(df: pd.DataFrame, volume_per_bar: int = 1000) -> pd.DataFrame:
    """Optimized volume-based bar building using numpy operations."""
    logger.info(f"Building volume bars with {volume_per_bar} volume per bar")
    
    df = filter_session_hours(df)
    df = df.sort_index()
    
    # Calculate cumulative volume
    cumulative_volume = df['qty'].cumsum()
    
    # Create base bar IDs using volume threshold
    base_bar_ids = cumulative_volume // volume_per_bar
    
    if NEW_BAR_AT_SESSION_START:
        # Find session starts
        session_starts = df.index.time == SESSION_START
        # Increment bar IDs after session starts
        bar_ids = base_bar_ids + np.cumsum(session_starts)
    else:
        bar_ids = base_bar_ids
    
    # Group and aggregate
    grouped = df.groupby(bar_ids)
    prices = grouped['price']
    volumes = grouped['qty']
    times = grouped.apply(lambda x: pd.Series({
        'bar_time_first': x.index[0],
        'bar_time_last': x.index[-1]
    }))
    
    # Combine results
    ohlcv = pd.DataFrame({
        'open': prices.first(),
        'high': prices.max(),
        'low': prices.min(),
        'close': prices.last(),
        'volume': volumes.sum(),
        'bar_time_first': times['bar_time_first'],
        'bar_time_last': times['bar_time_last']
    })
    
    logger.info(f"Generated {len(ohlcv)} volume bars")
    return ohlcv

def store_bars_arctic(symbol: str, df_bars: pd.DataFrame, suffix: str):
    """Store OHLCV bars in Arctic."""
    lib_symbol = f"{symbol}_bars_{suffix}"
    try:
        arctic_lib.write(lib_symbol, df_bars)
        logger.info(f"Successfully stored {len(df_bars)} bars to Arctic: {lib_symbol}")
    except Exception as e:
        logger.error(f"Failed to store bars to Arctic {lib_symbol}: {str(e)}")
        raise

############
# MAIN
############

def display_bars(df: pd.DataFrame, bar_type: str):
    """Display head and tail of bars with proper formatting."""
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.float_format', lambda x: '%.2f' % x)
    
    print(f"\n{'='*50}")
    print(f"{bar_type} Bars Preview")
    print('='*50)
    print("\nFirst 5 bars:")
    print(df.head())
    print("\nLast 5 bars:")
    print(df.tail())
    print(f"\nTotal {bar_type} bars: {len(df)}")
    print(f"Date range: {df.index[0]} to {df.index[-1]}")

if __name__ == "__main__":
    try:
        symbol = "NQH25_FUT_CME"
        logger.info(f"Starting bar generation for {symbol}")

        df_ticks = get_tick_data(symbol)
        
        # Build and display time bars
        df_timebars = build_time_bars(df_ticks, freq='1Min')
        display_bars(df_timebars, "Time (1-minute)")
        
        # Build and display trade bars
        df_tradebars = build_trade_bars(df_ticks, trades_per_bar=375)
        display_bars(df_tradebars, "Trade (375)")
        
        # Build and display volume bars
        df_volbars = build_volume_bars(df_ticks, volume_per_bar=750)
        display_bars(df_volbars, "Volume (750)")

        logger.info("\n=== Bar Statistics ===")
        logger.info(f"Time bars: {len(df_timebars)} bars generated")
        logger.info(f"Trade bars: {len(df_tradebars)} bars generated")
        logger.info(f"Volume bars: {len(df_volbars)} bars generated")

        store_bars_arctic(symbol, df_timebars, suffix='1min')
        store_bars_arctic(symbol, df_tradebars, suffix='trade375')
        store_bars_arctic(symbol, df_volbars, suffix='vol750')

        logger.info("Successfully completed bar generation and storage")
        
    except Exception as e:
        logger.error(f"Failed to complete bar generation: {str(e)}")
        raise