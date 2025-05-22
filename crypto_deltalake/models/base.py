from abc import ABC
from dataclasses import dataclass
from datetime import datetime


@dataclass
class DataModel(ABC):
    """Base class for all data models with common fields"""
    timestamp: datetime
    symbol: str