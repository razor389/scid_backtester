#!/usr/bin/env python3
"""
timestamp_utils.py

Utilities for converting Sierra Chart timestamps to datetimes (and back).
"""

from datetime    import datetime
from numpy       import datetime64, timedelta64
from json        import loads

# read config so we know the UTC offset
CONFIG = loads(open("./config.json").read())

# Build Sierra Chart's epoch + your local UTC offset
UTC_OFFSET_US = timedelta64(int(CONFIG["utc_offset"] * 3.6e9), "us")
SC_EPOCH      = datetime64("1899-12-30") + UTC_OFFSET_US

def ts_to_ds(ts: int, fmt: str = "%Y-%m-%d %H:%M:%S.%f") -> str:
    """
    Convert a Sierra Chart timestamp (microseconds since SC_EPOCH)
    to a human-readable datetime string.
    """
    # (SC_EPOCH + ts) => datetime64 => standard Python datetime => string
    return (SC_EPOCH + timedelta64(ts, "us")).astype(datetime).strftime(fmt)

def ds_to_ts(ds: str) -> int:
    """
    Convert a string datetime (YYYY-mm-dd HH:MM:SS, etc.) back into
    microseconds since SC_EPOCH for Sierra Chart timestamps.
    """
    # (ds => datetime64 => difference from SC_EPOCH).astype int64
    return (datetime64(ds) - SC_EPOCH).astype("int64")
