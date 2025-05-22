#!/usr/bin/env python3
"""Collect price data once and exit"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path to import crypto_deltalake
sys.path.insert(0, str(Path(__file__).parent.parent))

from crypto_deltalake.main import CryptoDataCollector


async def main():
    collector = CryptoDataCollector()
    await collector.collect_prices_once()


if __name__ == "__main__":
    asyncio.run(main())