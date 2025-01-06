#!/usr/bin/env python3
"""
sym_it.py

Synchronized iteration of T&S and Depth records for a single day.
"""
from bisect import bisect_right
from json   import loads
from time   import time

from parsers import depth_rec, tas_rec, parse_tas, parse_tas_header, parse_depth, parse_depth_header

SC_ROOT = loads(open("./config.json").read())["sc_root"]

class SymIt:

    def __init__(self, symbol: str, date: str, ts: int = 0):
        self.symbol = symbol
        self.date   = date
        self.ts     = ts

        self.sync       = False
        self.tas_recs   = []
        self.lob_recs   = []
        self.tas_i      = 0
        self.lob_i      = 0

        self.tas_fd = open(f"{SC_ROOT}/Data/{symbol}.scid", "rb")
        self.lob_fd = open(f"{SC_ROOT}/Data/MarketDepthData/{symbol}.{date}.depth", "rb")

        parse_tas_header(self.tas_fd)
        parse_depth_header(self.lob_fd)

    def synchronize(self, update: bool):
        if update:
            self.tas_recs += parse_tas(self.tas_fd, 0)
            self.lob_recs += parse_depth(self.lob_fd, 0)

        self.lob_i = bisect_right(self.lob_recs, self.ts, key=lambda r: r[depth_rec.timestamp])
        if self.lob_i < len(self.lob_recs):
            self.ts = self.lob_recs[self.lob_i][depth_rec.timestamp]
        else:
            self.ts = self.lob_recs[-1][depth_rec.timestamp]

        self.tas_i = bisect_right(self.tas_recs, self.ts, key=lambda r: r[tas_rec.timestamp])

    def set_ts(self, ts: int, update: bool = False):
        self.ts   = ts
        self.sync = False
        self.synchronize(update)

    def __iter__(self):
        self.synchronize(True)
        return self

    def __next__(self):
        if self.lob_i < len(self.lob_recs):
            if self.tas_i >= len(self.tas_recs) or \
               self.lob_recs[self.lob_i][depth_rec.timestamp] < self.tas_recs[self.tas_i][tas_rec.timestamp]:
                res = self.lob_recs[self.lob_i]
                self.ts = res[depth_rec.timestamp]
                self.lob_i += 1
            else:
                res = self.tas_recs[self.tas_i]
                self.ts = res[tas_rec.timestamp]
                self.tas_i += 1
        else:
            raise StopIteration
        return res

    def all(self):
        res = []
        old_ts = self.ts
        update = not (self.lob_recs or self.tas_recs)
        self.set_ts(0, update)
        for r in self:
            res.append(r)
        self.set_ts(old_ts)
        return res

    def __getitem__(self, slice):
        res = []
        if slice.start < len(self.lob_recs):
            # interpret it as a direct slice index
            res = self.all()[slice]
        else:
            # interpret as timestamp-based slice
            old_ts = self.ts
            update = not (self.lob_recs or self.tas_recs)
            self.set_ts(slice.start, update)
            for r in self:
                ts_val = r[0]  # the timestamp
                if ts_val <= slice.stop:
                    res.append(r)
            self.set_ts(old_ts)
        return res
