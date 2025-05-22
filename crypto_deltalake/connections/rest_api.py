import asyncio
from typing import AsyncGenerator, Dict, List, Optional

import aiohttp

from ..config.settings import get_logger
from .base import Connection

logger = get_logger(__name__)


class BinanceRestAPI(Connection):
    """REST API connection to Binance for price data"""

    def __init__(self, base_url: str = "https://api.binance.com/api/v3"):
        super().__init__(base_url)
        self.session: Optional[aiohttp.ClientSession] = None

    async def _ensure_session(self) -> aiohttp.ClientSession:
        """Ensure aiohttp session exists"""
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()
        return self.session

    async def close(self) -> None:
        """Close the aiohttp session"""
        if self.session and not self.session.closed:
            await self.session.close()

    async def get_ticker_price(self, symbol: str) -> Dict:
        """Get current price for a specific symbol"""
        session = await self._ensure_session()
        url = f"{self.base_url}/ticker/price"
        params = {"symbol": symbol}

        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Error fetching price for {symbol}: {e}")
            raise

    async def get_ticker_24hr(self, symbol: Optional[str] = None) -> List[Dict]:
        """Get 24hr ticker statistics for symbol(s)"""
        session = await self._ensure_session()
        url = f"{self.base_url}/ticker/24hr"
        params = {"symbol": symbol} if symbol else {}

        try:
            async with session.get(url, params=params) as response:
                response.raise_for_status()
                data = await response.json()
                return data if isinstance(data, list) else [data]
        except Exception as e:
            logger.error(f"Error fetching 24hr ticker: {e}")
            raise

    async def get_all_ticker_prices(self) -> List[Dict]:
        """Get current prices for all symbols"""
        session = await self._ensure_session()
        url = f"{self.base_url}/ticker/price"

        try:
            async with session.get(url) as response:
                response.raise_for_status()
                return await response.json()
        except Exception as e:
            logger.error(f"Error fetching all ticker prices: {e}")
            raise

    async def get_symbols_prices(self, symbols: List[str]) -> List[Dict]:
        """Get current prices for specific symbols"""
        tasks = [self.get_ticker_price(symbol) for symbol in symbols]
        return await asyncio.gather(*tasks, return_exceptions=True)

    async def get_enriched_ticker_data(self, symbols: Optional[List[str]] = None) -> List[Dict]:
        """Get enriched ticker data with 24hr statistics"""
        if symbols:
            # Get data for specific symbols
            tasks = [self.get_ticker_24hr(symbol) for symbol in symbols]
            results = await asyncio.gather(*tasks, return_exceptions=True)

            # Flatten results and filter out exceptions
            ticker_data = []
            for result in results:
                if isinstance(result, list):
                    ticker_data.extend(result)
                elif isinstance(result, dict):
                    ticker_data.append(result)
                else:
                    logger.warning(f"Skipping invalid result: {result}")

            return ticker_data
        else:
            # Get data for all symbols
            return await self.get_ticker_24hr()

    async def connect(self) -> AsyncGenerator[List[Dict], None]:
        """Connection method that yields all ticker data periodically"""
        while True:
            try:
                data = await self.get_enriched_ticker_data()
                yield data
                await asyncio.sleep(60)  # Wait 1 minute between requests
            except Exception as e:
                logger.error(f"Error in REST API connection: {e}")
                await asyncio.sleep(10)  # Wait before retry