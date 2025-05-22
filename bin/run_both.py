#!/usr/bin/env python3
"""Run both liquidation and price data collection"""

import asyncio
import sys
from pathlib import Path

# Add parent directory to path to import crypto_deltalake
sys.path.insert(0, str(Path(__file__).parent.parent))

from crypto_deltalake.main import CryptoDataCollector


async def main():
    collector = CryptoDataCollector()
    await collector.run_both()


if __name__ == "__main__":
    asyncio.run(main())