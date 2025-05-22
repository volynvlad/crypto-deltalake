import asyncio
import signal
from typing import List

from .config import LIQUIDATION_SCHEMA, PRICE_SCHEMA, settings
from .config.settings import get_logger
from .connections import BinanceRestAPI, BinanceWebSocket
from .processors import LiquidationProcessor, PriceProcessor
from .services import LiquidationService, PriceService
from .writers import DeltaTableWriter

logger = get_logger(__name__)


class CryptoDataCollector:
    """Main application coordinator for crypto data collection"""

    def __init__(self):
        self.services: List = []
        self._shutdown_event = asyncio.Event()

        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals"""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        asyncio.create_task(self._shutdown())

    async def _shutdown(self):
        """Graceful shutdown"""
        self._shutdown_event.set()

        # Stop all services
        for service in self.services:
            service.stop()

        logger.info("Shutdown complete")

    async def run_liquidations_only(self):
        """Run only liquidation data collection"""
        logger.info("Starting liquidation-only mode")

        # Create liquidation service
        liquidation_service = LiquidationService(
            connection=BinanceWebSocket(settings.binance_websocket_url),
            processor=LiquidationProcessor(),
            writer=DeltaTableWriter(settings.table_liquidation_path, LIQUIDATION_SCHEMA)
        )
        self.services.append(liquidation_service)

        # Run service
        await liquidation_service.start()

    async def run_prices_only(self):
        """Run only price data collection"""
        logger.info("Starting price-only mode")

        # Create price service
        price_service = PriceService(
            api=BinanceRestAPI(settings.binance_api_url),
            processor=PriceProcessor(),
            writer=DeltaTableWriter(settings.table_price_path, PRICE_SCHEMA)
        )
        self.services.append(price_service)

        # Run service
        await price_service.start_periodic_collection()

    async def run_both(self):
        """Run both liquidation and price data collection"""
        logger.info("Starting both liquidation and price collection")

        # Create services
        liquidation_service = LiquidationService(
            connection=BinanceWebSocket(settings.binance_websocket_url),
            processor=LiquidationProcessor(),
            writer=DeltaTableWriter(settings.table_liquidation_path, LIQUIDATION_SCHEMA)
        )

        price_service = PriceService(
            api=BinanceRestAPI(settings.binance_api_url),
            processor=PriceProcessor(),
            writer=DeltaTableWriter(settings.table_price_path, PRICE_SCHEMA)
        )

        self.services.extend([liquidation_service, price_service])

        # Run both services concurrently
        await asyncio.gather(
            liquidation_service.start(),
            price_service.start_periodic_collection(),
            self._wait_for_shutdown()
        )

    async def _wait_for_shutdown(self):
        """Wait for shutdown signal"""
        await self._shutdown_event.wait()

    async def collect_prices_once(self):
        """Collect price data once and exit"""
        logger.info("Collecting price data once")

        price_service = PriceService(
            api=BinanceRestAPI(settings.binance_api_url),
            processor=PriceProcessor(),
            writer=DeltaTableWriter(settings.table_price_path, PRICE_SCHEMA)
        )

        await price_service.collect_once()
        logger.info("Price collection completed")


async def main():
    """Main entry point"""
    collector = CryptoDataCollector()

    # For backward compatibility, run liquidations by default
    # You can modify this to run different modes
    await collector.run_liquidations_only()

    # Alternative modes:
    # await collector.run_prices_only()
    # await collector.run_both()
    # await collector.collect_prices_once()


if __name__ == "__main__":
    asyncio.run(main())