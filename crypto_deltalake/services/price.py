import asyncio
from typing import List, Optional

from ..config.settings import get_logger, settings
from ..connections.rest_api import BinanceRestAPI
from ..processors.price import PriceProcessor
from ..writers.delta import DeltaTableWriter

logger = get_logger(__name__)


class PriceService:
    """Service for collecting and storing price data"""

    def __init__(
        self,
        api: BinanceRestAPI,
        processor: PriceProcessor,
        writer: DeltaTableWriter,
        symbols: Optional[List[str]] = None
    ):
        self.api = api
        self.processor = processor
        self.writer = writer
        self.symbols = symbols  # None means all symbols
        self._running = False

    async def start_periodic_collection(
        self,
        interval_seconds: int = None
    ) -> None:
        """Start periodic price data collection"""
        interval_seconds = interval_seconds or settings.price_collection_interval
        logger.info(f"Starting price service with {interval_seconds}s interval")
        self._running = True

        try:
            while self._running:
                await self._collect_prices()
                await asyncio.sleep(interval_seconds)
        except Exception as e:
            logger.error(f"Error in price service: {e}")
            raise
        finally:
            await self.api.close()
            logger.info("Price service stopped")

    async def _collect_prices(self) -> None:
        """Collect prices once"""
        try:
            # Get price data
            if self.symbols:
                price_data = await self.api.get_enriched_ticker_data(self.symbols)
            else:
                price_data = await self.api.get_enriched_ticker_data()

            if not price_data:
                logger.warning("No price data received")
                return

            # Process data
            df = self.processor.process_batch(price_data)

            # Write to Delta table
            self.writer.write(df)

            # Optimize periodically
            self.writer.optimize()

            logger.info(f"Collected and stored {len(df)} price records")

        except Exception as e:
            logger.error(f"Error collecting prices: {e}")
            raise

    async def collect_once(self) -> None:
        """Collect prices once and stop"""
        try:
            await self._collect_prices()
        finally:
            await self.api.close()

    def stop(self) -> None:
        """Stop the price data collection service"""
        logger.info("Stopping price service")
        self._running = False

    def get_stats(self) -> dict:
        """Get service statistics"""
        return {
            "service": "price",
            "running": self._running,
            "symbols": self.symbols,
            "table_info": self.writer.get_table_info()
        }