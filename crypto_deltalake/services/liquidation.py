
from ..config.settings import get_logger
from ..connections.websocket import BinanceWebSocket
from ..processors.liquidation import LiquidationProcessor
from ..writers.delta import DeltaTableWriter

logger = get_logger(__name__)


class LiquidationService:
    """Service for collecting and storing liquidation data"""

    def __init__(
        self,
        connection: BinanceWebSocket,
        processor: LiquidationProcessor,
        writer: DeltaTableWriter
    ):
        self.connection = connection
        self.processor = processor
        self.writer = writer
        self._running = False

    async def start(self) -> None:
        """Start the liquidation data collection service"""
        logger.info("Starting liquidation service")
        self._running = True

        try:
            async for message in self.connection.get_liquidations():
                if not self._running:
                    break

                try:
                    # Process the message
                    df = self.processor.process_websocket_message(message)

                    # Write to Delta table
                    self.writer.write(df)

                    # Optimize periodically
                    self.writer.optimize()

                except Exception as e:
                    logger.error(f"Error processing liquidation message: {e}")
                    logger.error(f"Message: {message}")
                    continue

        except Exception as e:
            logger.error(f"Error in liquidation service: {e}")
            raise
        finally:
            logger.info("Liquidation service stopped")

    def stop(self) -> None:
        """Stop the liquidation data collection service"""
        logger.info("Stopping liquidation service")
        self._running = False

    def get_stats(self) -> dict:
        """Get service statistics"""
        return {
            "service": "liquidation",
            "running": self._running,
            "table_info": self.writer.get_table_info()
        }