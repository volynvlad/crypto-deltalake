"""
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams
"""
import json
import os
from datetime import datetime

from config import get_logger
from deltalake import DeltaTable, Field, write_deltalake
from deltalake import Schema as DlShema
from polars import DataFrame, Datetime, Float64, LazyFrame, Schema, String, read_delta, scan_delta

logger = get_logger(__name__)


reverse_type_mapping = {
    Float64: "double",
    String: "string",
    Datetime("us"): "timestamp",
}


class LiquidationsData:
    field_to_type = {
        "time": Datetime("us"),
        "symbol": String(),
        "side": String(),
        "original_quantity": Float64,
        "price": Float64,
        "avg_price": Float64,
        "status": String(),
        "last_filled_quantity": Float64,
        "filled_accum_quantity": Float64,
    }
    polars_schema = Schema(field_to_type)
    delta_schema = DlShema(
        [
            Field(name=name, type=reverse_type_mapping[dtype], nullable=False)  # type: ignore
            for name, dtype in field_to_type.items()
        ]
    )

    def __init__(self, table_path: str) -> None:
        self.table_path = table_path
        os.makedirs(os.path.dirname(self.table_path), exist_ok=True)
        logger.info(f"Table: {self.table_path} exists.")
        DeltaTable.create(self.table_path, self.delta_schema, mode="ignore")
        self.delta_table: DeltaTable = DeltaTable(self.table_path)


    def process(self, response):
        self.data = DataFrame(
            {
                "time": datetime.fromtimestamp(int(response["E"]) // 1000),
                "symbol": response['o']['s'],
                "side": response['o']['S'],
                "original_quantity": float(response['o']['q']),
                "price": float(response['o']['p']),
                "avg_price": float(response['o']['ap']),
                "status": response["o"]["X"],
                "last_filled_quantity": float(response["o"]["l"]),
                "filled_accum_quantity": float(response["o"]["z"]),
            },
            schema=self.polars_schema,
        )

    def write_data(self):
        write_deltalake(self.table_path, data=self.data, mode="append", schema_mode="merge")

    def optimize(self, force: bool = False, retention_hours: int = 0, dry_run: bool = False, z_order: list[str] | None = None):
        z_order = z_order or ["symbol"]
        if force or datetime.now().minute == 0:
            optimize_metrics = self.delta_table.optimize.z_order(z_order)
            logger.info(f"{optimize_metrics=}")
            self.delta_table.vacuum(retention_hours=retention_hours, dry_run=dry_run, enforce_retention_duration=False)

    def clean_metadata(self):
        self.delta_table.cleanup_metadata()

    def read_delta_table(self) -> DataFrame:
        return read_delta(self.table_path)

    def scan_delta_table(self) -> LazyFrame:
        return scan_delta(self.table_path)

    def run(self, response):
        try:
            self.process(json.loads(response))
        except Exception as e:
            logger.error(e)
            logger.error(f"{response=}")
            raise

        self.write_data()
        self.optimize()
