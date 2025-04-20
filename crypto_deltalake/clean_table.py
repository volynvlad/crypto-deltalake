from config import TABLE_LIQUIDATION_PATH
from data import LiquidationsData


def main():
    liquid_data = LiquidationsData(TABLE_LIQUIDATION_PATH)
    res = liquid_data.optimize(force=True, dry_run=True)
    liquid_data.delta_table.cleanup_metadata()
    print(res)


if __name__ == '__main__':
    main()
