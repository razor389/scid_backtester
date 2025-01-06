#!/usr/bin/env python3
"""
etl_arctic.py

Reads Sierra Chart .scid/.depth files (Time & Sales + Depth),
transforms them, and writes directly to Arctic (MongoDB).
It uses checkpoints in config.json to know where it left off.

Usage:
    python etl_arctic.py <loop>

Where:
    loop=0 => read and load once, then quit
    loop=1 => read/load continuously, sleeping between reads
"""
import os
import sys
import re
import asyncio
import time
import pandas as pd

from json    import loads, dumps
from os      import walk
from re      import match

# Arctic
from arctic import Arctic, TICK_STORE

# Local modules
from parsers import (
    parse_tas_header, parse_tas, transform_tas,
    parse_depth_header, parse_depth, transform_depth
)

# Load config
CONFIG = loads(open("./config.json").read())
CONTRACTS = CONFIG["contracts"]
SLEEP_INT = CONFIG["sleep_int"]
SC_ROOT   = CONFIG["sc_root"]

# Arctic setup
ARCTIC_HOST = "localhost"  # or "mongodb://your_user:your_pass@hostname"
LIB_NAME    = "tick_data"

# Initialize Arctic library
store = Arctic(ARCTIC_HOST)
if LIB_NAME not in store.list_libraries():
    store.initialize_library(LIB_NAME, lib_type=TICK_STORE)
arctic_lib = store[LIB_NAME]

###############
# HELPER FUNCTIONS
###############

def _records_to_df_tas(records):
    """
    Convert T&S records to a DataFrame with columns:
    timestamp, price, qty, side
    """
    if not records:
        return pd.DataFrame(columns=["timestamp", "price", "qty", "side"])
    df = pd.DataFrame(records, columns=["timestamp", "price", "qty", "side"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df

def _records_to_df_depth(records):
    """
    Convert Depth records to a DataFrame with columns:
    timestamp, command, flags, num_orders, price, quantity
    """
    if not records:
        return pd.DataFrame(columns=["timestamp", "command", "flags", "num_orders", "price", "quantity"])
    df = pd.DataFrame(records, columns=["timestamp", "command", "flags", "num_orders", "price", "quantity"])
    df = df.sort_values("timestamp").reset_index(drop=True)
    return df

def write_tas_arctic(symbol_name: str, records: list):
    """
    Write T&S records to Arctic under symbol: symbol_name + "_tas".
    """
    df = _records_to_df_tas(records)
    if not df.empty:
        lib_symbol = f"{symbol_name}_tas"
        # Append/Upsert in TICK_STORE
        arctic_lib.write(lib_symbol, df, upsert=True)

def write_depth_arctic(symbol_name: str, records: list):
    """
    Write Depth records to Arctic under symbol: symbol_name + "_depth".
    """
    df = _records_to_df_depth(records)
    if not df.empty:
        lib_symbol = f"{symbol_name}_depth"
        arctic_lib.write(lib_symbol, df, upsert=True)

################
# TIME & SALES
################

async def etl_tas_coro(
    con_id: str,
    checkpoint: int,
    price_adj: float,
    loop_mode: int
):
    """
    Parse SCID data from disk for one contract,
    transform, then write to Arctic.
    """
    fn = f"{SC_ROOT}/Data/{con_id}.scid"
    to_seek = checkpoint

    with open(fn, "rb") as fd:
        parse_tas_header(fd)

        while True:
            parsed = parse_tas(fd, to_seek)
            parsed = transform_tas(parsed, price_adj)

            # Write to Arctic
            write_tas_arctic(con_id, parsed)

            # Update checkpoint
            checkpoint += len(parsed)
            to_seek = 0

            if loop_mode:
                await asyncio.sleep(SLEEP_INT)
            else:
                break

    return (con_id, checkpoint)

async def etl_tas(loop_mode: int):
    """
    Orchestrate T&S ETL for all configured contracts.
    """
    coros = []
    for con_id, info in CONTRACTS.items():
        if info.get("tas", False):
            checkpoint = info["checkpoint_tas"]
            price_adj  = info["price_adj"]
            coros.append(etl_tas_coro(con_id, checkpoint, price_adj, loop_mode))

    results = await asyncio.gather(*coros)
    for con_id, new_cp in results:
        CONFIG["contracts"][con_id]["checkpoint_tas"] = new_cp

##############
# MARKET DEPTH
##############

async def etl_depth_coro(
    con_id: str,
    file_name: str,
    checkpoint: int,
    price_adj: float,
    loop_mode: int
):
    """
    Parse .depth data from disk for one day (one file),
    transform, then write to Arctic.
    """
    fn = f"{SC_ROOT}/Data/MarketDepthData/{file_name}"
    to_seek = checkpoint

    with open(fn, "rb") as fd:
        parse_depth_header(fd)

        while True:
            parsed = parse_depth(fd, to_seek)
            parsed = transform_depth(parsed, price_adj)

            # Write to Arctic
            write_depth_arctic(con_id, parsed)

            checkpoint += len(parsed)
            to_seek = 0

            if loop_mode:
                await asyncio.sleep(SLEEP_INT)
            else:
                break

    return (file_name, checkpoint)

async def etl_depth(loop_mode: int):
    """
    Orchestrate depth ETL for all configured contracts.
    """
    _, _, files = next(walk(f"{SC_ROOT}/Data/MarketDepthData"))

    for con_id, info in CONTRACTS.items():
        if not info.get("depth", False):
            continue

        price_adj       = info["price_adj"]
        checkpoint_date = info["checkpoint_depth"]["date"]
        checkpoint_rec  = info["checkpoint_depth"]["rec"]

        # Identify which files to parse
        to_parse = []
        for f in files:
            parts = f.split(".")  # e.g. [ESU22_FUT_CME, 20220906, depth]
            if match(con_id, parts[0]) and parts[1] >= checkpoint_date:
                to_parse.append(f)

        to_parse = sorted(to_parse)
        if not to_parse:
            continue

        coros = []
        for depth_file in to_parse:
            # checkpoint applies only to the earliest file
            cp = checkpoint_rec if checkpoint_date in depth_file else 0
            # loop_mode applies only to the last (most recent) file
            mod = loop_mode if depth_file == to_parse[-1] else 0

            coros.append(etl_depth_coro(con_id, depth_file, cp, price_adj, mod))

        results = await asyncio.gather(*coros)

        # The last result => the final checkpoint
        if results:
            last_file, last_checkpoint = results[-1]
            new_date = last_file.split(".")[1]
            CONFIG["contracts"][con_id]["checkpoint_depth"]["date"] = new_date
            CONFIG["contracts"][con_id]["checkpoint_depth"]["rec"] = last_checkpoint

###########
# MAIN
###########

async def main():
    start_time = time.time()

    loop_mode = int(sys.argv[1]) if len(sys.argv) > 1 else 0

    # Run T&S and Depth ETLs concurrently
    task_tas   = etl_tas(loop_mode)
    task_depth = etl_depth(loop_mode)
    await asyncio.gather(task_tas, task_depth)

    # Write updated checkpoints to config
    with open("./config.json", "w") as fd:
        fd.write(dumps(CONFIG, indent=2))

    print(f"Elapsed: {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    asyncio.run(main())
