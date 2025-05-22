try:
    from .config import LIQUIDATION_SCHEMA, PRICE_SCHEMA, settings
    from .writers import DeltaTableWriter
except ImportError:
    from config import LIQUIDATION_SCHEMA, PRICE_SCHEMA, settings
    from writers import DeltaTableWriter


def clean_liquidation_table():
    """Clean and optimize liquidation table"""
    print("=== Cleaning Liquidation Table ===")

    liquidation_writer = DeltaTableWriter(settings.table_liquidation_path, LIQUIDATION_SCHEMA)

    try:
        # Force optimization
        print("Optimizing liquidation table...")
        result = liquidation_writer.optimize(force=True, dry_run=False)
        print(f"Optimization result: {result}")

        # Cleanup metadata
        print("Cleaning up metadata...")
        liquidation_writer.cleanup_metadata()

        # Show table info
        info = liquidation_writer.get_table_info()
        print(f"Table info: {info}")

    except Exception as e:
        print(f"Error cleaning liquidation table: {e}")


def clean_price_table():
    """Clean and optimize price table"""
    print("\n=== Cleaning Price Table ===")

    price_writer = DeltaTableWriter(settings.table_price_path, PRICE_SCHEMA)

    try:
        # Force optimization
        print("Optimizing price table...")
        result = price_writer.optimize(force=True, dry_run=False)
        print(f"Optimization result: {result}")

        # Cleanup metadata
        print("Cleaning up metadata...")
        price_writer.cleanup_metadata()

        # Show table info
        info = price_writer.get_table_info()
        print(f"Table info: {info}")

    except Exception as e:
        print(f"Error cleaning price table: {e}")


def main():
    """Main cleanup function"""
    clean_liquidation_table()
    clean_price_table()
    print("\nCleanup completed!")


if __name__ == '__main__':
    main()