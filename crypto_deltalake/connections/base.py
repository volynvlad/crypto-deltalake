from abc import ABC, abstractmethod
from typing import Any, AsyncGenerator


class Connection(ABC):
    """Abstract base class for all connections"""

    def __init__(self, base_url: str):
        self.base_url = base_url

    @abstractmethod
    async def connect(self) -> AsyncGenerator[Any, None]:
        """Establish connection and yield data"""
        pass