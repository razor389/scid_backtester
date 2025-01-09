-> data
    -s3 cloud storage of .scid .depth -> arcticdb lmdb
    we want to mimic sc, in that we only want to make what we need when we need it:
        -downsampling: bars (time, tick, volume)
        -organization: contracts, rolled price series (back adjustment), spreads (back adjustment)
        -visualization: bars
        -volume/delta at price (arctic ok here?)
        -volume profile (rolling volume profile)

-> signals:
    -recreate PA signals (can we have closed source signal testing?)
    -continuous signals (with signal in units of e.g. sharpe ratio)

-> backtesting / parameter sweep optimization
    -3 pass: load bars/book from .scid/.depth, then signals, then backtest
