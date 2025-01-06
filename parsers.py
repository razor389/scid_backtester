#!/usr/bin/env python3
"""
parsers.py

Contains parsing logic for Sierra Chart .scid (Time & Sales) and
.depth (Market Depth) files. Also includes basic transformations.
"""
from enum        import IntEnum
from numpy       import datetime64
from os          import fstat
from struct      import calcsize, Struct
from typing      import BinaryIO, List

SC_EPOCH = datetime64("1899-12-30")

# Time and Sales
class intraday_rec(IntEnum):
    timestamp   = 0
    open        = 1
    high        = 2
    low         = 3
    close       = 4
    num_trades  = 5
    total_vol   = 6
    bid_vol     = 7
    ask_vol     = 8

class tas_rec(IntEnum):
    timestamp   = 0
    price       = 1
    qty         = 2
    side        = 3

# Binary formats from Sierra Chart docs
INTRADAY_HEADER_FMT  = "4cIIHHI36c"
INTRADAY_HEADER_LEN  = calcsize(INTRADAY_HEADER_FMT)

INTRADAY_REC_FMT     = "q4f4I"
INTRADAY_REC_LEN     = calcsize(INTRADAY_REC_FMT)
INTRADAY_REC_UNPACK  = Struct(INTRADAY_REC_FMT).unpack_from

def parse_tas_header(fd: BinaryIO) -> tuple:
    header_bytes = fd.read(INTRADAY_HEADER_LEN)
    header = Struct(INTRADAY_HEADER_FMT).unpack_from(header_bytes)
    return header

def parse_tas(fd: BinaryIO, checkpoint: int) -> List:
    """
    Parse tick-by-tick records from .scid file. Each returned
    record is (timestamp, price, quantity, side).
    side=0 => trade at bid, side=1 => trade at ask.
    """
    fstat(fd.fileno())  # not strictly necessary, but can be used to get file size
    tas_recs = []

    if checkpoint:
        fd.seek(INTRADAY_HEADER_LEN + checkpoint * INTRADAY_REC_LEN)

    while intraday_rec_bytes := fd.read(INTRADAY_REC_LEN):
        ir = INTRADAY_REC_UNPACK(intraday_rec_bytes)

        # In T&S mode, "close" is the actual trade price,
        # and "bid_vol"/"ask_vol" indicates side and quantity
        trade_side = 0 if ir[intraday_rec.bid_vol] > 0 else 1
        trade_qty  = ir[intraday_rec.bid_vol] if ir[intraday_rec.bid_vol] else ir[intraday_rec.ask_vol]

        rec = (
            ir[intraday_rec.timestamp],
            ir[intraday_rec.close],
            trade_qty,
            trade_side
        )
        tas_recs.append(rec)

    return tas_recs

def transform_tas(rs: List, price_adj: float):
    """
    Adjust raw trades, applying price multiplier (if needed).
    Return a list of (timestamp, price, qty, side).
    """
    return [
        (
            r[tas_rec.timestamp],
            r[tas_rec.price] * price_adj,
            r[tas_rec.qty],
            r[tas_rec.side]
        )
        for r in rs
    ]

# Market Depth
class depth_rec(IntEnum):
    timestamp   = 0
    command     = 1
    flags       = 2
    num_orders  = 3
    price       = 4
    quantity    = 5
    reserved    = 6

class depth_cmd(IntEnum):
    none        = 0
    clear_book  = 1
    add_bid_lvl = 2
    add_ask_lvl = 3
    mod_bid_lvl = 4
    mod_ask_lvl = 5
    del_bid_lvl = 6
    del_ask_lvl = 7

DEPTH_HEADER_FMT  = "4I48c"
DEPTH_HEADER_LEN  = calcsize(DEPTH_HEADER_FMT)

DEPTH_REC_FMT     = "qBBHfII"
DEPTH_REC_LEN     = calcsize(DEPTH_REC_FMT)
DEPTH_REC_UNPACK  = Struct(DEPTH_REC_FMT).unpack_from

def parse_depth_header(fd: BinaryIO) -> tuple:
    header_bytes = fd.read(DEPTH_HEADER_LEN)
    header = Struct(DEPTH_HEADER_FMT).unpack_from(header_bytes)
    return header

def parse_depth(fd: BinaryIO, checkpoint: int) -> List:
    """
    Parse market depth records from .depth file into a list of tuples:
    (timestamp, command, flags, num_orders, price, quantity).
    """
    fstat(fd.fileno())
    depth_recs = []

    if checkpoint:
        fd.seek(DEPTH_HEADER_LEN + checkpoint * DEPTH_REC_LEN)

    while depth_rec_bytes := fd.read(DEPTH_REC_LEN):
        dr = DEPTH_REC_UNPACK(depth_rec_bytes)
        depth_recs.append(dr)

    return depth_recs

def transform_depth(rs: List, price_adj: float):
    """
    Adjust prices by multiplier, remove the 'reserved' field,
    and return a list of:
    (timestamp, command, flags, num_orders, price, quantity)
    """
    return [
        (
            r[depth_rec.timestamp],
            r[depth_rec.command],
            r[depth_rec.flags],
            r[depth_rec.num_orders],
            r[depth_rec.price] * price_adj,
            r[depth_rec.quantity]
        )
        for r in rs
    ]
