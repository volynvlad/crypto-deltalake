from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar

from polars import DataFrame

from ..models.base import DataModel

T = TypeVar('T', bound=DataModel)


class DataProcessor(ABC, Generic[T]):
    """Abstract base class for data processors"""

    @abstractmethod
    def process_single(self, raw_data: dict) -> T:
        """Process single raw data item into model"""
        pass

    @abstractmethod
    def to_dataframe(self, models: List[T]) -> DataFrame:
        """Convert list of models to Polars DataFrame"""
        pass

    def process_batch(self, raw_data_list: List[dict]) -> DataFrame:
        """Process batch of raw data into DataFrame"""
        models = [self.process_single(item) for item in raw_data_list]
        return self.to_dataframe(models)