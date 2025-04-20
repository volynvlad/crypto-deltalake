import json
from enum import StrEnum
from typing import AsyncGenerator

import websockets
from config import get_logger

logger = get_logger(__name__)

BINANCE_FSTREAM_URL = "wss://fstream.binance.com/ws"


class EventType(StrEnum):
    FORCE_ORDER = "!forceOrder@arr"
    KLINE = "kline_{interval}".format(interval="1m")
    TRADE = "trade"


async def get_liquidations() -> AsyncGenerator:
    """
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
    async with websockets.connect(BINANCE_FSTREAM_URL) as websocket:
        await websocket.send(json.dumps(subscribe_message))
        while True:
            response = await websocket.recv()
            data = json.loads(response)
            logger.debug(f"{data=}")
            if "error" in data:
                logger.error(data["error"])
            if "result" in data or "E" not in data:
                continue
            yield response
