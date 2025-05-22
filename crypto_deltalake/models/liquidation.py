from dataclasses import dataclass
from datetime import datetime

from .base import DataModel


@dataclass
class LiquidationModel(DataModel):
    """Model for liquidation order data from Binance WebSocket"""
    side: str
    original_quantity: float
    price: float
    avg_price: float
    status: str
    last_filled_quantity: float
    filled_accum_quantity: float

    # Rename timestamp to time for compatibility
    @property
    def time(self) -> datetime:
        return self.timestamp