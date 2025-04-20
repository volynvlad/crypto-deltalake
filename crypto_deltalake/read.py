import polars as pl
from config import TABLE_LIQUIDATION_PATH
from data import LiquidationsData


def main():
    liquid_data = LiquidationsData(TABLE_LIQUIDATION_PATH)
    df = liquid_data.scan_delta_table()
    btcusdt_df = df.sort("time").filter(pl.col("symbol") == "BTCUSDT")
    print("BUY: ", btcusdt_df.filter(pl.col("side") == "BUY").count().collect())
    print("SELL: ", btcusdt_df.filter(pl.col("side") == "SELL").count().collect())
    print(
        df
        .group_by(["symbol", "side"])
        .count()
        .filter(pl.col("count") > 10)
        .sort("count", descending=True)
        .collect()
    )
    print(
        df
        .filter(pl.col("price") * pl.col("original_quantity") > 100_000)
        .collect()
    )
    print(df.collect())


if __name__ == '__main__':
    main()
