#!/usr/bin/env python3
"""
update_file_list.py

Given start (e.g. N23) and end (e.g. Z23) contract codes,
generates a list of file IDs for bulk downloading in Sierra Chart.

Usage:
    python update_file_list.py N23 Z23
"""
from sys import argv

SYMBOLS = [
    # ( "CL{MYY}_FUT_CME", "FGHJKMNQUVXZ", True ), # etc...
    # ...
    ( "ES{MYY}_FUT_CME", "HMUZ", True ),
    # add more as needed...
]

if __name__ == "__main__":
    start = (argv[1][0], argv[1][1:])
    end   = (argv[2][0], argv[2][1:])
    years = [str(i) for i in range(int(start[1]), int(end[1]) + 1)]

    for symbol in SYMBOLS:
        if not symbol[-1]:
            continue

        pattern = symbol[0]
        months  = symbol[1]
        opt     = "OPT" in pattern

        for year in years:
            for month in months:
                myy = month + year

                if (year == start[1] and month < start[0]) or \
                   (year == end[1]   and month > end[0]):
                    continue

                if not opt:
                    contract_id = pattern.format(MYY=myy)
                    print(contract_id)
                else:
                    strike_def = symbol[2].split(":")
                    lo_strike  = int(strike_def[0])
                    hi_strike  = int(strike_def[1])
                    increment  = int(strike_def[2])
                    fill_width = int(strike_def[3])

                    for ttype in ["C", "P"]:
                        for i in range(lo_strike, hi_strike + increment, increment):
                            contract_id = pattern.format(
                                MYY=myy, 
                                T=ttype, 
                                S=str(i).rjust(fill_width, "0")
                            )
                            print(contract_id)
