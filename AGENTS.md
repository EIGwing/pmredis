# PMRedis Agent Guidelines

This document provides guidelines for agentic coding agents working on the PMRedis codebase.

## Project Overview

PMRedis collects real-time market data from Polymarket, Binance, and Chainlink, storing it in Redis streams with a 10-minute rolling window. It's a Python project using Redis for data storage.

## Project Structure

```
pmredis/
├── main.py                 # Entry point, DataCollectorRedis class
├── pmredis/
│   ├── collectors/          # Data collectors (polymarket, binance, chainlink)
│   │   ├── base.py         # BaseCollector class
│   │   ├── polymarket.py   # PolymarketCollector
│   │   ├── polymarket_stream.py
│   │   ├── chainlink.py
│   │   ├── chainlink_stream.py
│   │   └── binance.py
│   ├── storage/
│   │   └── redis_manager.py # RedisManager class
│   └── utils/
│       └── helpers.py
├── Dockerfile
├── docker-compose.yml
└── requirements.txt
```

## Build & Run Commands

### Installation (with virtual environment recommended)
```bash
# Create and activate virtual environment
python3 -m venv venv
source venv/bin/activate  # On macOS/Linux
# or: venv\Scripts\activate  # On Windows

# Install dependencies
pip install -r requirements.txt

# Optional: install playwright browsers (for polymarket_stream)
playwright install chromium
```

### Running the Application
```bash
# Activate virtual environment first
source venv/bin/activate

# Start with defaults (localhost:6379)
python main.py start

# Dry-run mode (no Redis required, logs to console)
python main.py start --dry-run

# Specify Redis host and port
python main.py start --redis-host redishost --redis-port 6380

# Specify which tables to collect
python main.py start --tables polymarket_prices,polymarket_orderbook

# Adjust sample interval (default: 0.2s = 5Hz)
python main.py start --sample-interval 0.1

# View help
python main.py help
```

### Running with Docker
```bash
docker-compose up --build
```

## Testing

**No test framework is currently configured.** When adding tests:

```bash
# Install pytest
pip install pytest pytest-cov

# Run all tests
pytest

# Run a single test file
pytest tests/test_redis_manager.py

# Run a single test function
pytest tests/test_redis_manager.py::TestRedisManager::test_connect

# Run with coverage
pytest --cov=pmredis --cov-report=html
```

## Code Style Guidelines

### General
- Python 3.10+
- Use type hints for all function parameters and return types
- Use docstrings for all public functions and classes
- Use logging (not print) for runtime output

### Naming Conventions
- **Classes**: PascalCase (e.g., `RedisManager`, `DataCollectorRedis`)
- **Functions/variables**: snake_case (e.g., `connect()`, `redis_host`)
- **Constants**: UPPER_SNAKE_CASE (e.g., `TEN_MINUTES_MS`)
- **Private methods**: prefix with underscore (e.g., `_make_request()`)

### Imports
- Standard library imports first
- Third-party imports second
- Local imports third
- Group by type, alphabetically within each group

```python
import json
import logging
import time
from typing import Any, Dict, List, Optional

import pandas as pd
import requests

from pmredis.collectors.base import BaseCollector
```

### Type Hints
Use `typing` module for type hints:
- `Optional[X]` instead of `X | None` (for Python 3.9 compatibility)
- Use `Dict`, `List`, `Tuple` for collections (or use built-ins for Python 3.9+)
- Always specify return types for functions

```python
def connect(self) -> bool:
    ...

def xadd(self, stream_key: str, data: Dict[str, Any]) -> Optional[str]:
    ...
```

### Error Handling
- Catch specific exceptions when possible
- Log errors appropriately (logger.error for failures, logger.warning for recoverable issues)
- Return sensible defaults (None, empty list/dict, False) on error
- Include context in error messages

```python
try:
    response = self.session.get(url, timeout=30)
    response.raise_for_status()
except requests.RequestException as e:
    logger.error(f"Request failed: {e}")
    return None
```

### Logging
- Use module-level logger: `logger = logging.getLogger(__name__)`
- Use appropriate log levels:
  - `logger.debug()` - detailed diagnostic info
  - `logger.info()` - normal operation events
  - `logger.warning()` - unexpected but handled issues
  - `logger.error()` - serious problems

### Formatting
- Line length: 100 characters max
- Use f-strings for string formatting
- Use trailing commas in multi-line structures

```python
stream_key = get_stream_key("polymarket_prices")
self.redis.xadd(stream_key, record)

record = {
    "timestamp": str(ts_ms),
    "source_timestamp": str(source_ts),
    "session_epoch": str(current_epoch * 1000),
}
```

### Docstrings
Use Google-style docstrings:

```python
def xadd(self, stream_key: str, data: Dict[str, Any]) -> Optional[str]:
    """Add a record to a stream.

    Args:
        stream_key: Redis stream key (e.g., "data:polymarket_orderbook")
        data: Dictionary of field-value pairs

    Returns:
        Message ID if successful, None otherwise
    """
```

### Redis Operations
- Always check connection before operations
- Handle reconnection gracefully
- Use stream keys with prefix: `data:{table_name}`
- Implement trimming for rolling window (default: 10 minutes)

## Linting & Formatting

No linting tools are currently configured. Recommended additions:

```bash
# Install tools
pip install ruff black mypy

# Run formatter
black pmredis/ main.py

# Run linter
ruff check pmredis/ main.py

# Type checking
mypy pmredis/ main.py
```

## Adding New Collectors

1. Inherit from `BaseCollector` in `pmredis/collectors/base.py`
2. Implement required methods
3. Add import to `pmredis/collectors/__init__.py`
4. Integrate into `DataCollectorRedis` in `main.py`

## Key Configuration

- Redis host: configurable via `--redis-host`
- Redis port: configurable via `--redis-port`
- Sample interval: configurable via `--sample-interval` (default 0.2s = 5Hz)
- Tables: configurable via `--tables`
