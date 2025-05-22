from dataclasses import dataclass
from typing import Optional

from .base import DataModel


@dataclass
class PriceModel(DataModel):
    """Model for price ticker data from Binance REST API"""
    price: float
    volume_24h: Optional[float] = None
    price_change_24h: Optional[float] = None
    price_change_percent_24h: Optional[float] = None
    high_24h: Optional[float] = None
    low_24h: Optional[float] = None