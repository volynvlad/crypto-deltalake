"""
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
"""
from datetime import datetime
import json
import os

from deltalake import DeltaTable

from config import get_logger
from polars import DataFrame, Datetime, Float64, Schema, String


logger = get_logger(__name__)


class WriteLiquidations:
    schema = Schema({
        "time": Datetime("us"),
        "symbol": String(),
        "side": String(),
        "original_quantity": Float64,
        "price": Float64,
        "avg_price": Float64,
    })
    def __init__(self, table_path: str) -> None:
        self.table_path = table_path
        os.makedirs(os.path.dirname(self.table_path), exist_ok=True)
        logger.info(f"Table: {self.table_path} exists.")

    def process(self, response):
        self.data = DataFrame(
            {
                "time": datetime.fromtimestamp(int(response["E"]) // 1000),
                "symbol": response['o']['s'],
                "side": response['o']['S'],
                "original_quantity": float(response['o']['q']),
                "price": float(response['o']['p']),
                "avg_price": float(response['o']['ap']),
            },
            schema=self.schema,
        )

    def write_data(self):
        self.data.write_delta(self.table_path, mode="append")

    def optimize(self):
        if datetime.now().hour == 0:
            dt = DeltaTable(self.table_path)
            optimize_metrics = dt.optimize.compact()
            logger.info(f"{optimize_metrics=}")

    def run(self, response):
        try:
            self.process(json.loads(response))
        except Exception as e:
            logger.error(e)
            logger.error(f"{response=}")
            raise

        self.write_data()
        self.optimize()
