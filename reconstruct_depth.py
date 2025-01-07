#!/usr/bin/env python3
"""
reconstruct_depth.py

Reconstructs the last FULLY COMPLETED snapshot (from CLEAR_BOOK
to FLAG_END_OF_BATCH) that ends at or before a user-specified
target timestamp (Sierra Chart microseconds).

This avoids partial snapshots, which can happen if you stop
after the last CLEAR_BOOK but haven't yet seen FLAG_END_OF_BATCH.
"""

import pandas as pd
from arcticdb import Arctic

# Sierra Chart Depth Command Enums
CMD_CLEAR_BOOK       = 1
CMD_ADD_BID_LEVEL    = 2
CMD_ADD_ASK_LEVEL    = 3
CMD_MODIFY_BID_LEVEL = 4
CMD_MODIFY_ASK_LEVEL = 5
CMD_DELETE_BID_LEVEL = 6
CMD_DELETE_ASK_LEVEL = 7

# Flag
FLAG_END_OF_BATCH = 0x01


def apply_depth_update(bids: dict, asks: dict, row: pd.Series):
    """
    Applies one market depth command to the current order book in-place.
    `bids` and `asks` are dicts: price -> (quantity, num_orders).
    """
    cmd        = row['command']
    price      = row['price']
    quantity   = row['quantity']
    num_orders = row['num_orders']

    if cmd == CMD_CLEAR_BOOK:
        bids.clear()
        asks.clear()

    elif cmd == CMD_ADD_BID_LEVEL:
        bids[price] = (quantity, num_orders)

    elif cmd == CMD_ADD_ASK_LEVEL:
        asks[price] = (quantity, num_orders)

    elif cmd == CMD_MODIFY_BID_LEVEL:
        bids[price] = (quantity, num_orders)  # treat missing as ADD

    elif cmd == CMD_MODIFY_ASK_LEVEL:
        asks[price] = (quantity, num_orders)

    elif cmd == CMD_DELETE_BID_LEVEL:
        if price in bids:
            del bids[price]

    elif cmd == CMD_DELETE_ASK_LEVEL:
        if price in asks:
            del asks[price]


def find_completed_snapshots(df_depth: pd.DataFrame):
    snapshots = []
    snapshot_start_idx = None
    snapshot_start_ts = None

    for idx, row in df_depth.iterrows():
        cmd = row['command']
        # Force row['flags'] to Python int
        flg = int(row['flags'])  
        ts  = row['timestamp']

        if cmd == CMD_CLEAR_BOOK:
            snapshot_start_idx = idx
            snapshot_start_ts  = ts

        if snapshot_start_idx is not None and (flg & FLAG_END_OF_BATCH) != 0:
            snapshot_end_idx = idx
            snapshot_end_ts  = ts
            snapshots.append((snapshot_start_idx, snapshot_end_idx, snapshot_start_ts, snapshot_end_ts))
            snapshot_start_idx = None
            snapshot_start_ts = None

    return snapshots


def reconstruct_book_last_full_snapshot(df_depth: pd.DataFrame, target_ts: int, max_depth: int = 10):
    """
    1) Identify all fully completed snapshots: each from CLEAR_BOOK to a row with FLAG_END_OF_BATCH.
    2) Find the last snapshot whose `end_ts` <= target_ts.
    3) Replay that snapshot to build the final book state.

    Returns (snapshot_end_ts, bids_top, asks_top).

    If no fully completed snapshot ends before target_ts, returns an empty book.
    """

    # 1) List all fully completed snapshot ranges
    snapshots = find_completed_snapshots(df_depth)
    if not snapshots:
        return None, [], []

    # 2) Find the last snapshot whose end_ts <= target_ts
    valid = [(s, e, sts, ets) for (s, e, sts, ets) in snapshots if ets <= target_ts]
    if not valid:
        # No snapshot fully completed by target_ts
        return None, [], []

    # pick the snapshot with the largest end_ts
    last_snap = max(valid, key=lambda x: x[3])
    start_idx, end_idx, start_ts, end_ts = last_snap

    # 3) Extract rows for that snapshot, then replay in ascending order of index
    df_snap = df_depth.loc[start_idx:end_idx].copy()
    df_snap.sort_index(inplace=True)  # ensure ascending index (if not already)
    # (We could also sort by timestamp if needed, but typically index order is the file order.)

    # Reconstruct
    bids = {}
    asks = {}

    for idx, row in df_snap.iterrows():
        apply_depth_update(bids, asks, row)

    # Now we have the final book at the end of that snapshot
    snapshot_time = end_ts

    # Sort and slice
    # Bids: descending by price
    bids_sorted = sorted(bids.items(), key=lambda x: x[0], reverse=True)
    bids_top = [(p, q[0], q[1]) for p, q in bids_sorted[:max_depth]]

    # Asks: ascending by price
    asks_sorted = sorted(asks.items(), key=lambda x: x[0])
    asks_top = [(p, q[0], q[1]) for p, q in asks_sorted[:max_depth]]

    return snapshot_time, bids_top, asks_top


def main():
    # 1) Connect to Arctic, read the actual depth data
    ARCTIC_HOST = "mongodb://localhost:27017"
    LIB_NAME    = "tick_data"
    DEPTH_SYMBOL = "NQH25_FUT_CME_depth"

    print("Connecting to Arctic...")
    from arcticdb import Arctic
    store = Arctic(ARCTIC_HOST)
    arctic_lib = store[LIB_NAME]

    print(f"Reading depth data: {DEPTH_SYMBOL}")
    df_depth = arctic_lib.read(DEPTH_SYMBOL).data

    # DEBUG: Show original dtype
    print("Original dtype:", df_depth['flags'].dtype)

    # Convert
    df_depth['flags'] = pd.to_numeric(df_depth['flags'], errors='coerce').fillna(0).astype(int)

    # DEBUG: Confirm conversion
    print("After conversion:", df_depth['flags'].dtype)
    print("Unique flags:", df_depth['flags'].unique())

    # OPTIONAL: Check a few rows
    print(df_depth[['flags']].head(10))

    print("Data loaded. Shape:", df_depth.shape)

    # 2) Choose your target timestamp (Sierra Chart microseconds)
    #    For instance, let's pick the last row's timestamp or any you desire:
    target_ts = df_depth['timestamp'].iloc[-20]  # e.g. the final row

    # 3) Reconstruct the LAST *fully completed* snapshot that ended <= target_ts
    snapshot_ts, bids_top, asks_top = reconstruct_book_last_full_snapshot(df_depth, target_ts, max_depth=10)

    if snapshot_ts is None:
        print(f"No fully completed snapshot found <= {target_ts}.")
        return

    # 4) Print results
    print(f"\n=== FULL SNAPSHOT RECONSTRUCTION ===")
    print(f"Target TS: {target_ts}")
    print(f"Snapshot End TS: {snapshot_ts}")

    print("\nTop Bids (descending by price):")
    for (price, qty, orders) in bids_top:
        print(f"  {price:.2f}  qty={qty}  #orders={orders}")

    print("\nTop Asks (ascending by price):")
    for (price, qty, orders) in asks_top:
        print(f"  {price:.2f}  qty={qty}  #orders={orders}")


if __name__ == "__main__":
    main()
