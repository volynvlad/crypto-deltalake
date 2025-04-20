import polars as pl
from config import TABLE_LIQUIDATION_PATH

df = pl.read_delta(TABLE_LIQUIDATION_PATH)
print(df)
