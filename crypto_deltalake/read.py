import polars as pl

try:
    from .config import LIQUIDATION_SCHEMA, PRICE_SCHEMA, settings
    from .writers import DeltaTableWriter
except ImportError:
    from config import LIQUIDATION_SCHEMA, PRICE_SCHEMA, settings
    from writers import DeltaTableWriter


def analyze_liquidations():
    """Analyze liquidation data"""
    print("=== Liquidation Data Analysis ===")

    # Create writer to access data
    liquidation_writer = DeltaTableWriter(settings.table_liquidation_path, LIQUIDATION_SCHEMA)

    try:
        df = liquidation_writer.scan_data()

        # BTCUSDT analysis
        btcusdt_df = df.filter(pl.col("symbol") == "BTCUSDT").sort("time")
        print("BTCUSDT BUY count:", btcusdt_df.filter(pl.col("side") == "BUY").select(pl.len()).collect().item())
        print("BTCUSDT SELL count:", btcusdt_df.filter(pl.col("side") == "SELL").select(pl.len()).collect().item())

        # Top liquidations by symbol and side
        print("\nTop liquidations (count > 10):")
        top_liquidations = (df
            .group_by(["symbol", "side"])
            .len()
            .filter(pl.col("len") > 10)
            .sort("len", descending=True)
            .collect()
        )
        print(top_liquidations)

        # Large liquidations (> $100k)
        print("\nLarge liquidations (> $100k):")
        large_liquidations = (df
            .filter(pl.col("price") * pl.col("original_quantity") > 100_000)
            .select(["time", "symbol", "side", "price", "original_quantity"])
            .sort("time", descending=True)
            .collect()
        )
        print(large_liquidations)

        # Table info
        print("\nTable info:")
        info = liquidation_writer.get_table_info()
        print(f"Rows: {info.get('row_count', 'Unknown')}")
        print(f"Version: {info.get('version', 'Unknown')}")

    except Exception as e:
        print(f"Error reading liquidation data: {e}")


def analyze_prices():
    """Analyze price data"""
    print("\n=== Price Data Analysis ===")

    # Create writer to access data
    price_writer = DeltaTableWriter(settings.table_price_path, PRICE_SCHEMA)

    try:
        df = price_writer.scan_data()

        # Basic stats
        total_records = df.select(pl.len()).collect().item()
        print(f"Total price records: {total_records}")

        if total_records > 0:
            # Latest prices for top symbols
            print("\nLatest prices for major symbols:")
            major_symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "ADAUSDT", "SOLUSDT"]

            latest_prices = (df
                .filter(pl.col("symbol").is_in(major_symbols))
                .group_by("symbol")
                .agg([
                    pl.col("timestamp").max().alias("latest_time"),
                    pl.col("price").last().alias("latest_price"),
                    pl.col("price_change_percent_24h").last().alias("change_24h")
                ])
                .sort("symbol")
                .collect()
            )
            print(latest_prices)

            # Price volatility analysis
            print("\nTop 10 most volatile symbols (24h):")
            volatile = (df
                .filter(pl.col("price_change_percent_24h").is_not_null())
                .group_by("symbol")
                .agg([
                    pl.col("price_change_percent_24h").last().alias("volatility")
                ])
                .sort("volatility", descending=True)
                .head(10)
                .collect()
            )
            print(volatile)

        # Table info
        print("\nPrice table info:")
        info = price_writer.get_table_info()
        print(f"Rows: {info.get('row_count', 'Unknown')}")
        print(f"Version: {info.get('version', 'Unknown')}")

    except Exception as e:
        print(f"Error reading price data: {e}")


def main():
    """Main analysis function"""
    analyze_liquidations()
    analyze_prices()


if __name__ == '__main__':
    main()