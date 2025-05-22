from abc import ABC, abstractmethod

from polars import DataFrame


class DataWriter(ABC):
    """Abstract base class for data writers"""

    @abstractmethod
    def write(self, data: DataFrame) -> None:
        """Write DataFrame to storage"""
        pass

    @abstractmethod
    def optimize(self, **kwargs) -> None:
        """Optimize storage"""
        pass