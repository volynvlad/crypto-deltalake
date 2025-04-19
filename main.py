"""
https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Liquidation-Order-Streams

"""
import asyncio
import itertools
import json
from datetime import datetime
from enum import StrEnum
from os import getenv, makedirs
from os.path import exists
from typing import Any

import websockets
from deltalake import DeltaTable
from dotenv import load_dotenv
from polars import DataFrame, Datetime, Float64, Schema, String
from websockets.exceptions import ConnectionClosed

from logger import get_logger

assert load_dotenv(".env"), ".env not loaded"

logger = get_logger(__name__)
API_KEY = getenv("API_KEY", "")

SYMBOL = "ethusdt"

BASE_POINT_SYMBOL = "wss://stream.binance.com/ws/{symbol}@{event_type}"

TABLES_PATH = "tables/liquidation"


#TODO
# - Create separate files
# - README


class EventType(StrEnum):
    FORCE_ORDER = "!forceOrder@arr"
    KLINE = "kline_{interval}".format(interval="1m")
    TRADE = "trade"


def on_open(ws):
    logger.info("Connection opened")


def on_close(ws, status_code, close_message):
    logger.info(f"Closed with status code: {status_code}. Message: {close_message}")


def on_message(ws, message: str):
    logger.info(message)


def on_error(ws, error):
    logger.error(error)


def process_response(data: dict[str, Any], schema: Schema) -> DataFrame:
    return DataFrame(
        {
            "time": datetime.fromtimestamp(data["E"] // 1000),
            "symbol": data['o']['s'],
            "side": data['o']['S'],
            "original_quantity": float(data['o']['q']),
            "price": float(data['o']['p']),
            "avg_price": float(data['o']['ap']),
        },
        schema=schema,
    )


async def keepalive(websocket, ping_interval=30):
    for ping in itertools.count():
        await asyncio.sleep(ping_interval)
        request = json.dumps({"ping": ping})
        try:
            await websocket.send(request)
        except ConnectionClosed as e:
            logger.error(f"{request=}. ConnectionClosed: {e}")
            break


async def get_liquidations(schema):
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
    url = "wss://fstream.binance.com/ws"

    async with websockets.connect(url) as websocket:
        subscribe_message = {
            "method": "SUBSCRIBE",
            "params": [EventType.FORCE_ORDER],
            "id": 1,
        }
        await websocket.send(json.dumps(subscribe_message))
        while True:
            response = await websocket.recv()
            data = json.loads(response)
            logger.debug(f"{data=}")
            if "error" in data:
                logger.error(data["error"])
            if "result" in data or "E" not in data:
                continue
            df = process_response(data, schema)
            df.write_delta(TABLES_PATH, mode="append")

            if datetime.now().hour == 0:
                dt = DeltaTable(TABLES_PATH)
                dt.optimize.compact()
                dt.vacuum(retention_hours=24)


def main():
    if not exists(TABLES_PATH):
        makedirs(TABLES_PATH)
    schema = Schema({
        "time": Datetime("us"),
        "symbol": String(),
        "side": String(),
        "original_quantity": Float64,
        "price": Float64,
        "avg_price": Float64,
    })
    asyncio.run(get_liquidations(schema))


if __name__ == "__main__":
    main()
