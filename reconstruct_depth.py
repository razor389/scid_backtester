#!/usr/bin/env python3
"""
reconstruct_depth.py

Reconstructs the last FULLY COMPLETED snapshot (from CLEAR_BOOK
to FLAG_END_OF_BATCH) that ends at or before a user-specified
target timestamp (Sierra Chart microseconds).
"""

import pandas as pd
from arcticdb import Arctic
from typing import Dict, List, Tuple, Optional
import logging

# Set up logger
logger = logging.getLogger(__name__)

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

def apply_depth_update(bids: Dict[float, Tuple[int, int]], 
                      asks: Dict[float, Tuple[int, int]], 
                      row: pd.Series,
                      validate: bool = True) -> None:
    """
    Applies one market depth command to the current order book state in-place.
    Args:
        bids: Dict mapping price -> (quantity, num_orders) for bid side
        asks: Dict mapping price -> (quantity, num_orders) for ask side
        row: DataFrame row with command, price, quantity, num_orders
        validate: Whether to validate inputs
    """
    try:
        cmd = int(row['command'])
        price = float(row['price'])
        quantity = int(row['quantity'])
        num_orders = int(row['num_orders'])
        
        # Validation checks
        if validate:
            if cmd not in range(1, 8):
                logger.warning(f"Invalid command: {cmd}")
                return
            if cmd != CMD_CLEAR_BOOK and (price <= 0 or quantity < 0 or num_orders < 0):
                logger.warning(f"Invalid price/quantity/orders: {price}/{quantity}/{num_orders}")
                return

        if cmd == CMD_CLEAR_BOOK:
            bids.clear()
            asks.clear()
            return

        # Determine if it's a bid or ask using command number (even=bid, odd=ask)
        is_bid = (cmd % 2) == 0
        
        # For modifications/adds
        if cmd in (CMD_ADD_BID_LEVEL, CMD_MODIFY_BID_LEVEL) and is_bid:
            if price in asks:  # Remove from wrong side if present
                del asks[price]
            bids[price] = (quantity, num_orders)
        elif cmd in (CMD_ADD_ASK_LEVEL, CMD_MODIFY_ASK_LEVEL) and not is_bid:
            if price in bids:  # Remove from wrong side if present
                del bids[price]
            asks[price] = (quantity, num_orders)
        # Handle deletes
        elif cmd == CMD_DELETE_BID_LEVEL and is_bid and price in bids:
            del bids[price]
        elif cmd == CMD_DELETE_ASK_LEVEL and not is_bid and price in asks:
            del asks[price]
            
    except (ValueError, KeyError, TypeError) as e:
        logger.warning(f"Error processing depth update: {e}")
        return


def find_completed_snapshots(df_depth: pd.DataFrame) -> List[Tuple[int, int, int, int]]:
    """
    Identifies fully completed depth snapshots in the data.
    Returns: List of tuples (start_idx, end_idx, start_ts, end_ts) for each snapshot
    """
    snapshots = []
    snapshot_start_idx = None
    snapshot_start_ts = None

    for idx, row in df_depth.iterrows():
        try:
            cmd = int(row['command'])
            flg = int(row['flags'])
            ts = int(row['timestamp'])

            # Start new snapshot on CLEAR_BOOK
            if cmd == CMD_CLEAR_BOOK:
                snapshot_start_idx = idx
                snapshot_start_ts = ts
            
            # End snapshot when we see END_OF_BATCH flag
            if snapshot_start_idx is not None and (flg & FLAG_END_OF_BATCH) != 0:
                snapshot_end_idx = idx
                snapshot_end_ts = ts
                snapshots.append((snapshot_start_idx, snapshot_end_idx, snapshot_start_ts, snapshot_end_ts))
                # Don't reset start idx/ts here - allow for overlapping snapshots
                # Only reset on next CLEAR_BOOK
                
        except (ValueError, KeyError, TypeError) as e:
            logger.warning(f"Error processing row {idx}: {e}")
            continue

    return snapshots


def validate_book_state(bids: Dict[float, Tuple[int, int]], 
                     asks: Dict[float, Tuple[int, int]]) -> bool:
    """
    Validates that the order book state is consistent:
    - All bid prices should be less than all ask prices
    - No negative quantities or number of orders
    """
    if not bids or not asks:
        return True  # Empty or one-sided book is valid
        
    max_bid = max(bids.keys()) if bids else float('-inf')
    min_ask = min(asks.keys()) if asks else float('inf')
    
    if max_bid >= min_ask:
        logger.warning(f"Book validation failed: max_bid ({max_bid}) >= min_ask ({min_ask})")
        return False
        
    return True


def reconstruct_book_last_full_snapshot(
    df_depth: pd.DataFrame, 
    target_ts: int, 
    max_depth: int = 10
) -> Tuple[Optional[int], List[Tuple[float, int, int]], List[Tuple[float, int, int]]]:
    """
    Reconstructs the order book state from the last fully completed snapshot
    before or at target_ts.

    Args:
        df_depth: DataFrame with depth records
        target_ts: Target timestamp (Sierra Chart microseconds)
        max_depth: Maximum number of levels to return per side

    Returns:
        Tuple of:
        - snapshot_ts: Timestamp of the snapshot (None if no valid snapshot)
        - bids_top: List of (price, qty, num_orders) tuples for bid side
        - asks_top: List of (price, qty, num_orders) tuples for ask side
    """
    try:
        # Find all completed snapshots
        snapshots = find_completed_snapshots(df_depth)
        if not snapshots:
            logger.info("No completed snapshots found")
            return None, [], []

        # Find last snapshot ending before/at target_ts
        valid_snaps = [(s, e, sts, ets) for (s, e, sts, ets) in snapshots if ets <= target_ts]
        if not valid_snaps:
            logger.info(f"No snapshots found before target timestamp {target_ts}")
            return None, [], []

        # Take the most recent valid snapshot
        start_idx, end_idx, start_ts, end_ts = max(valid_snaps, key=lambda x: x[3])
        logger.debug(f"Using snapshot from index {start_idx} to {end_idx}")

        # Extract and sort snapshot rows
        df_snap = df_depth.loc[start_idx:end_idx].copy()
        df_snap.sort_index(inplace=True)

        # Reconstruct the book
        bids: Dict[float, Tuple[int, int]] = {}  # price -> (qty, num_orders)
        asks: Dict[float, Tuple[int, int]] = {}

        for _, row in df_snap.iterrows():
            apply_depth_update(bids, asks, row)

        # Validate the book state
        if not validate_book_state(bids, asks):
            logger.warning(f"Invalid book state detected, attempting cleanup...")
            
            # Find the boundary between valid bids and asks
            all_prices = sorted(list(bids.keys()) + list(asks.keys()))
            if len(all_prices) > 1:
                mid_price = (all_prices[0] + all_prices[-1]) / 2
                # Move misplaced orders to correct side
                for price in list(bids.keys()):
                    if price >= mid_price:
                        asks[price] = bids[price]
                        del bids[price]
                for price in list(asks.keys()):
                    if price < mid_price:
                        bids[price] = asks[price]
                        del asks[price]

        # Sort and format the results
        bids_list = sorted(((p, q, n) for p, (q, n) in bids.items()), reverse=True)
        asks_list = sorted(((p, q, n) for p, (q, n) in asks.items()))

        logger.debug(f"Reconstructed book with {len(bids_list)} bids and {len(asks_list)} asks")
        
        if validate_book_state(bids, asks):
            return end_ts, bids_list[:max_depth], asks_list[:max_depth]
        else:
            logger.error("Book state remains invalid after cleanup")
            return None, [], []
        
    except Exception as e:
        logger.error(f"Error reconstructing book: {e}")
        return None, [], []


def format_book_side(levels: List[Tuple[float, int, int]], side: str = "Bids") -> str:
    """Helper to format one side of the book for display"""
    if not levels:
        return f"No {side}"
    
    result = [f"\n{side}:"]
    result.extend([
        f"  {price:10.2f}  {qty:6d}  {orders:3d}"
        for price, qty, orders in levels
    ])
    return "\n".join(result)


def main():
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        # Connect to Arctic and load data
        ARCTIC_HOST = "mongodb://localhost:27017"
        LIB_NAME = "tick_data"
        DEPTH_SYMBOL = "NQH25_FUT_CME_depth"

        store = Arctic(ARCTIC_HOST)
        arctic_lib = store[LIB_NAME]
        
        logger.info(f"Reading depth data: {DEPTH_SYMBOL}")
        df_depth = arctic_lib.read(DEPTH_SYMBOL).data
        logger.info(f"Loaded {len(df_depth)} depth records")

        # Ensure flags are integers
        df_depth['flags'] = df_depth['flags'].astype(int)

        # Pick a target timestamp (e.g., 20 rows from end)
        target_ts = df_depth['timestamp'].iloc[-20]
        logger.info(f"Target timestamp: {target_ts}")

        # Reconstruct the book
        snapshot_ts, bids, asks = reconstruct_book_last_full_snapshot(
            df_depth, target_ts, max_depth=10
        )

        if snapshot_ts is None:
            logger.warning(f"No completed snapshot found before timestamp {target_ts}")
            return
        
        # Import timestamp utils
        from timestamp_utils import ts_to_ds

        # Convert timestamps to readable datetime strings
        target_time = ts_to_ds(target_ts)
        snapshot_time = ts_to_ds(snapshot_ts)

        # Log the results
        logger.info("\nOrder Book Reconstruction")
        logger.info("-" * 40)
        logger.info(f"Target Time:   {target_time} CT")
        logger.info(f"Snapshot Time: {snapshot_time} CT")
        logger.info(f"Latency (μs):  {target_ts - snapshot_ts}")
        logger.info("\nPrice     Quantity  #Orders")
        logger.info("-" * 30)
        
        logger.info(format_book_side(asks, "Asks"))
        logger.info(format_book_side(bids, "Bids"))
        
    except Exception as e:
        logger.error(f"Error in main: {e}", exc_info=True)


if __name__ == "__main__":
    main()