import json
from enum import StrEnum
from typing import AsyncGenerator

import websockets

from ..config.settings import get_logger
from .base import Connection

logger = get_logger(__name__)


class EventType(StrEnum):
    """Binance WebSocket event types"""
    FORCE_ORDER = "!forceOrder@arr"
    KLINE_1M = "kline_1m"
    TRADE = "trade"


class BinanceWebSocket(Connection):
    """WebSocket connection to Binance futures streams"""

    def __init__(self, base_url: str = "wss://fstream.binance.com/ws"):
        super().__init__(base_url)

    async def get_liquidations(self) -> AsyncGenerator[str, None]:
        """
        Connect to Binance liquidation stream and yield messages

        Example liquidation message:
        {
          "e": "forceOrder",      // Event Type
          "E": 1568014460893,     // Event Time
          "o": {
            "s": "BTCUSDT",       // Symbol
            "S": "SELL",          // Side
            "o": "LIMIT",         // Order Type
            "f": "IOC",           // Time in Force
            "q": "0.014",         // Original Quantity
            "p": "9910",          // Price
            "ap": "9910",         // Average Price
            "X": "FILLED",        // Order Status
            "l": "0.014",         // Order Last Filled Quantity
            "z": "0.014",         // Order Filled Accumulated Quantity
            "T": 1568014460893    // Order Trade Time
          }
        }
        """
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [EventType.FORCE_ORDER],
            "id": 1,
        }

        async with websockets.connect(self.base_url) as websocket:
            await websocket.send(json.dumps(subscribe_message))
            logger.info("Subscribed to liquidation stream")

            while True:
                try:
                    response = await websocket.recv()
                    data = json.loads(response)
                    logger.debug(f"Received: {data}")

                    if "error" in data:
                        logger.error(f"WebSocket error: {data['error']}")
                        continue

                    if "result" in data or "E" not in data:
                        # Skip subscription confirmation and non-data messages
                        continue

                    yield response

                except websockets.exceptions.ConnectionClosed:
                    logger.warning("WebSocket connection closed, reconnecting...")
                    break
                except Exception as e:
                    logger.error(f"WebSocket error: {e}")
                    break

    async def connect(self) -> AsyncGenerator[str, None]:
        """Default connection method for liquidations"""
        async for message in self.get_liquidations():
            yield message