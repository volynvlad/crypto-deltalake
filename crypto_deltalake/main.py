import asyncio

from config import TABLE_LIQUIDATION_PATH
from connection import get_liquidations
from data import LiquidationsData

#TODO
# - Create separate files
# - README


async def main():
    write_liquidations = LiquidationsData(TABLE_LIQUIDATION_PATH)
    async for response in get_liquidations():
        write_liquidations.run(response)


if __name__ == "__main__":
    asyncio.run(main())
