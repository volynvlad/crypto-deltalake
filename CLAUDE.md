# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a modular cryptocurrency data pipeline that collects both liquidation data from Binance WebSocket streams and price data from Binance REST API, storing everything in Delta Lake format using Polars for data processing. The architecture follows clean separation of concerns with distinct layers:

### Core Architecture

1. **Models Layer** (`models/`): Data models for liquidation and price data with type safety
2. **Processors Layer** (`processors/`): Transform raw API data into structured DataFrames
3. **Writers Layer** (`writers/`): Handle Delta Lake storage with optimization
4. **Connections Layer** (`connections/`): WebSocket and REST API connections to Binance
5. **Services Layer** (`services/`): Business logic orchestrators for data collection
6. **Config Layer** (`config/`): Centralized settings and schema management

### Data Flow

- **Liquidations**: WebSocket → JSON → LiquidationModel → DataFrame → Delta Lake
- **Prices**: REST API → JSON → PriceModel → DataFrame → Delta Lake
- Both support real-time streaming and batch processing

## Development Commands

**Run liquidation data collection only:**
```bash
uv run crypto_deltalake/main.py
# or
uv run bin/run_liquidations.py
```

**Run price data collection only:**
```bash
uv run bin/run_prices.py
```

**Run both liquidation and price collection:**
```bash
uv run bin/run_both.py
```

**Collect price data once and exit:**
```bash
uv run bin/collect_prices_once.py
```

**Read and analyze stored data:**
```bash
uv run crypto_deltalake/read.py
```

**Clean and optimize tables:**
```bash
uv run crypto_deltalake/clean_table.py
```

**Check table storage information:**
```bash
./bin/check_tables.sh
```

**Lint code:**
```bash
uv run ruff check
```

**Format code:**
```bash
uv run ruff format
```

**Set up Jupyter kernel:**
```bash
uv add ipykernel --dev
python -m ipykernel install --user --name=crypto_deltalake --display-name=crypto_deltalake
```

## Key Classes and Components

### Service Classes
- `LiquidationService`: Orchestrates WebSocket → Processing → Storage for liquidations
- `PriceService`: Orchestrates REST API → Processing → Storage for prices
- `CryptoDataCollector`: Main application coordinator with graceful shutdown

### Data Models
- `LiquidationModel`: Structured liquidation order data
- `PriceModel`: Structured price ticker data with 24hr statistics
- All models inherit from `DataModel` base class

### Processors
- `LiquidationProcessor`: Converts WebSocket JSON to LiquidationModel and DataFrame
- `PriceProcessor`: Converts REST API JSON to PriceModel and DataFrame
- Support both single message and batch processing

### Writers
- `DeltaTableWriter`: Generic Delta Lake writer with optimization, vacuum, and metadata cleanup
- Supports Z-order optimization (defaults to 'symbol' column)
- Automatic hourly optimization and configurable retention

### Connections
- `BinanceWebSocket`: Async WebSocket client for liquidation streams
- `BinanceRestAPI`: Async HTTP client for price data with session management
- Both support reconnection and error handling

## Configuration

- **Settings**: Centralized in `config/settings.py` using dataclass
- **Environment Variables**: Loaded via .env file (LOG_TO_FILE, LOG_TO_TERMINAL, API_KEY)
- **Table Paths**: `tables/liquidation` and `tables/prices`
- **Schemas**: Type-safe schema definitions with Polars/Delta Lake mapping
- **Price Collection**: Default 60-second interval, configurable

## Architecture Benefits

- **Modularity**: Each component has single responsibility
- **Extensibility**: Easy to add new data sources or processors
- **Type Safety**: Full type hints and structured data models
- **Testability**: Clear interfaces for mocking and testing
- **Reusability**: Generic writers and processors for different data types
- **Observability**: Comprehensive logging throughout the pipeline
- **Reliability**: Graceful error handling and automatic reconnection

## Key Dependencies

- `deltalake`: Delta Lake storage format
- `polars`: High-performance data processing 
- `websockets`: Async WebSocket client for Binance streams
- `aiohttp`: Async HTTP client for REST API calls
- `python-dotenv`: Environment variable management