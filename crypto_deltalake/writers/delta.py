import os
from datetime import datetime
from typing import List, Optional

import polars as pl
from deltalake import DeltaTable, write_deltalake
from deltalake import Schema as DeltaSchema
from polars import DataFrame, LazyFrame, read_delta, scan_delta

from ..config.settings import get_logger
from .base import DataWriter

logger = get_logger(__name__)


class DeltaTableWriter(DataWriter):
    """Delta Lake table writer with optimization capabilities"""

    def __init__(self, table_path: str, schema: DeltaSchema):
        self.table_path = table_path
        self.schema = schema
        self._ensure_table_exists()
        self.delta_table = DeltaTable(self.table_path)

    def _ensure_table_exists(self) -> None:
        """Ensure table directory and Delta table exist"""
        os.makedirs(os.path.dirname(self.table_path), exist_ok=True)
        logger.info(f"Ensuring table exists: {self.table_path}")
        DeltaTable.create(self.table_path, self.schema, mode="ignore")

    def write(self, data: DataFrame) -> None:
        """Write DataFrame to Delta table"""
        if data.is_empty():
            logger.warning("Attempted to write empty DataFrame")
            return

        write_deltalake(
            self.table_path,
            data=data,
            mode="append",
            schema_mode="merge"
        )
        logger.debug(f"Written {len(data)} rows to {self.table_path}")

    def optimize(
        self,
        force: bool = False,
        retention_hours: int = 0,
        dry_run: bool = False,
        z_order: Optional[List[str]] = None
    ) -> dict:
        """Optimize Delta table with Z-order and vacuum"""
        z_order = z_order or ["symbol"]

        if force or datetime.now().minute == 0:
            logger.info(f"Optimizing table {self.table_path}")

            # Z-order optimization
            optimize_metrics = self.delta_table.optimize.z_order(z_order)
            logger.info(f"Optimization metrics: {optimize_metrics}")

            # Vacuum old files
            vacuum_metrics = self.delta_table.vacuum(
                retention_hours=retention_hours,
                dry_run=dry_run,
                enforce_retention_duration=False
            )
            logger.info(f"Vacuum completed: {vacuum_metrics}")

            return {
                "optimize": optimize_metrics,
                "vacuum": vacuum_metrics
            }

        return {}

    def cleanup_metadata(self) -> None:
        """Clean up Delta table metadata"""
        self.delta_table.cleanup_metadata()
        logger.info(f"Metadata cleanup completed for {self.table_path}")

    def read_data(self) -> DataFrame:
        """Read entire Delta table as DataFrame"""
        return read_delta(self.table_path)

    def scan_data(self) -> LazyFrame:
        """Scan Delta table as LazyFrame for efficient querying"""
        return scan_delta(self.table_path)

    def get_table_info(self) -> dict:
        """Get basic information about the table"""
        try:
            df = self.scan_data()
            count = df.select(pl.len()).collect().item()
            return {
                "path": self.table_path,
                "row_count": count,
                "version": self.delta_table.version(),
                "schema": self.delta_table.schema()
            }
        except Exception as e:
            logger.error(f"Error getting table info: {e}")
            return {"path": self.table_path, "error": str(e)}