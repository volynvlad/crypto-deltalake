from datetime import datetime
from typing import List, Optional

from polars import DataFrame

from ..config.schemas import PRICE_POLARS_SCHEMA
from ..models.price import PriceModel
from .base import DataProcessor


class PriceProcessor(DataProcessor[PriceModel]):
    """Processor for Binance price ticker data"""

    def process_single(self, raw_data: dict) -> PriceModel:
        """Process single price ticker data into model"""
        # Try different price fields based on API endpoint
        price = raw_data.get("lastPrice") or raw_data.get("price")
        if price is None:
            raise ValueError(f"No price field found in data: {list(raw_data.keys())}")

        return PriceModel(
            timestamp=datetime.now(),
            symbol=raw_data["symbol"],
            price=float(price),
            volume_24h=self._safe_float(raw_data.get("volume")),
            price_change_24h=self._safe_float(raw_data.get("priceChange")),
            price_change_percent_24h=self._safe_float(raw_data.get("priceChangePercent")),
            high_24h=self._safe_float(raw_data.get("highPrice")),
            low_24h=self._safe_float(raw_data.get("lowPrice")),
        )

    def to_dataframe(self, models: List[PriceModel]) -> DataFrame:
        """Convert price models to DataFrame"""
        if not models:
            return DataFrame(schema=PRICE_POLARS_SCHEMA)

        data = {
            "timestamp": [model.timestamp for model in models],
            "symbol": [model.symbol for model in models],
            "price": [model.price for model in models],
            "volume_24h": [model.volume_24h for model in models],
            "price_change_24h": [model.price_change_24h for model in models],
            "price_change_percent_24h": [model.price_change_percent_24h for model in models],
            "high_24h": [model.high_24h for model in models],
            "low_24h": [model.low_24h for model in models],
        }

        return DataFrame(data, schema=PRICE_POLARS_SCHEMA)

    @staticmethod
    def _safe_float(value: Optional[str]) -> Optional[float]:
        """Safely convert string to float, return None if invalid"""
        if value is None:
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None