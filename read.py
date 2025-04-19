import polars as pl

from main import TABLES_PATH

df = pl.read_delta(TABLES_PATH)
print(df)
