"""Base collector class."""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)


class BaseCollector:
    """Base class for all data collectors."""

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update(
            {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            }
        )

    def _make_request(
        self,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        max_retries: int = 3,
        retry_delay: int = 5,
    ) -> Optional[Dict[str, Any]]:
        """Make HTTP request with retry logic for rate limiting and errors."""
        for attempt in range(max_retries):
            try:
                response = self.session.get(url, params=params, timeout=30)

                if response.status_code == 429:
                    logger.warning(f"Rate limited. Waiting {retry_delay} seconds...")
                    time.sleep(retry_delay)
                    continue

                response.raise_for_status()
                return response.json()

            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed: {e}")
                if attempt < max_retries - 1:
                    logger.info(f"Retrying in {retry_delay} seconds...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"Max retries reached for {url}")
                    return None
        return None
