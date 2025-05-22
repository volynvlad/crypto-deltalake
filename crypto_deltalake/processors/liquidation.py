import json
from datetime import datetime
from typing import List

from polars import DataFrame

from ..config.schemas import LIQUIDATION_POLARS_SCHEMA
from ..models.liquidation import LiquidationModel
from .base import DataProcessor


class LiquidationProcessor(DataProcessor[LiquidationModel]):
    """Processor for Binance liquidation WebSocket data"""

    def process_single(self, raw_data: dict) -> LiquidationModel:
        """Process single liquidation message into model"""
        if isinstance(raw_data, str):
            raw_data = json.loads(raw_data)

        return LiquidationModel(
            timestamp=datetime.fromtimestamp(int(raw_data["E"]) // 1000),
            symbol=raw_data['o']['s'],
            side=raw_data['o']['S'],
            original_quantity=float(raw_data['o']['q']),
            price=float(raw_data['o']['p']),
            avg_price=float(raw_data['o']['ap']),
            status=raw_data["o"]["X"],
            last_filled_quantity=float(raw_data["o"]["l"]),
            filled_accum_quantity=float(raw_data["o"]["z"]),
        )

    def to_dataframe(self, models: List[LiquidationModel]) -> DataFrame:
        """Convert liquidation models to DataFrame"""
        if not models:
            return DataFrame(schema=LIQUIDATION_POLARS_SCHEMA)

        data = {
            "time": [model.time for model in models],
            "symbol": [model.symbol for model in models],
            "side": [model.side for model in models],
            "original_quantity": [model.original_quantity for model in models],
            "price": [model.price for model in models],
            "avg_price": [model.avg_price for model in models],
            "status": [model.status for model in models],
            "last_filled_quantity": [model.last_filled_quantity for model in models],
            "filled_accum_quantity": [model.filled_accum_quantity for model in models],
        }

        return DataFrame(data, schema=LIQUIDATION_POLARS_SCHEMA)

    def process_websocket_message(self, message: str) -> DataFrame:
        """Process single WebSocket message and return DataFrame"""
        model = self.process_single(message)
        return self.to_dataframe([model])