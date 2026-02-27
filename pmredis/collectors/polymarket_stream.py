import asyncio
import logging
import time
import threading
from typing import Optional

from playwright.async_api import async_playwright

TRACE = 5
logging.addLevelName(TRACE, "TRACE")

logger = logging.getLogger(__name__)


def get_current_epoch() -> int:
    now = int(time.time())
    return now - (now % 300)


def get_url(epoch: int = None) -> str:
    if epoch is None:
        epoch = get_current_epoch()
    return f"https://polymarket.com/event/btc-updown-5m-{epoch}"


SELECTORS = {
    "price_to_beat": [
        "#price-chart-container > div > div > div:nth-child(1) > div > div.flex.w-max > div:nth-child(1) > span",
        "#price-chart-container > div > div > div:nth-child(1) > div > div.flex.w-max > div > span",
        "#price-chart-container > div > div > div:nth-child(1) > div > div.flex.gap-5.w-max > div:nth-child(1) > span",
        "#price-chart-container [class*='text-'] span",
        "#price-chart-container > div > div > div:first-child span",
    ],
    "current_price": [
        "#price-chart-container > div > div > div:nth-child(2) > div > div > div > div:nth-child(1) > div:nth-child(2)",
        "#price-chart-container .group\\/legend-item span",
    ],
    "timestamp": [
        "#price-chart-container > div > div > div:nth-child(2) > div > div > div > div:nth-child(2) > div:nth-child(2)",
        "#price-chart-container .text-gray-500",
    ],
}


class PolymarketStreamScraper:
    def __init__(self, check_interval_ms: int = 250, stale_threshold: int = 30):
        self._browser = None
        self._playwright = None
        self._context = None

        self._tooltip_page = None  # Page 1: current_price + chart_time (continuous)
        self._ptb_page = None  # Page 2: price_to_beat (session-based)
        self._tooltip_page_url: str = ""  # Current URL of tooltip page

        self._current_epoch: int = 0

        self._price_to_beat: Optional[float] = None  # Official price_to_beat
        self._waiting_for_ptb: bool = False  # True when waiting for official PTB
        self._waiting_for_ptb_start: float = 0  # Start time of waiting for PTB
        self._ptb_pending_epoch: int = 0  # Epoch we're waiting for PTB

        self._last_current_price: Optional[float] = None
        self._last_chart_timestamp: str = ""

        self._last_data_change_time: float = 0  # Last time price/chart changed
        self._last_hover_time: float = 0
        self._hover_x_percent: float = 0.885

        self._check_interval_ms = check_interval_ms
        self._stale_threshold = stale_threshold

        self._callbacks = []
        self._is_recovering = False
        self._reload_event = threading.Event()

        self._page_load_time: float = 0  # Time when tooltip page was last loaded

    def add_callback(self, callback):
        self._callbacks.append(callback)

    def _notify_callbacks(
        self, price: float, price_to_beat: Optional[float], timestamp: str
    ):
        for callback in self._callbacks:
            try:
                callback(price, price_to_beat, timestamp, self._tooltip_page_url)
            except Exception as e:
                logger.debug(f"Callback error: {e}")

    async def _init_browser(self):
        if self._playwright is None:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-gpu",
                    "--disable-dev-shm-usage",
                    "--disable-extensions",
                    "--disable-background-networking",
                    "--disable-default-apps",
                    "--disable-sync",
                    "--disable-translate",
                    "--metrics-recording-only",
                    "--no-first-run",
                    "--single-process",
                    "--memory-pressure-off",
                    "--disable-features=TranslateUI",
                    "--disable-ipc-flooding-protection",
                    "--disable-renderer-backgrounding",
                    "--disable-background-timer-throttling",
                    "--disable-backgrounding-occluded-windows",
                    "--disable-hang-monitor",
                    "--disable-prompt-on-repost",
                    "--disable-popup-blocking",
                    "--disable-permission-blocklist",
                    "--disable-accelerated-2d-canvas",
                    "--media-cache-size=0",
                    "--disk-cache-size=0",
                ],
            )
            self._context = await self._browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            )
            logger.info("Browser launched with context")

    async def _create_tooltip_page(self):
        """Create or recreate the tooltip page for current_price + chart_time."""
        logger.info("Creating tooltip page...")
        if self._tooltip_page:
            await self._tooltip_page.close()

        self._tooltip_page = await self._context.new_page()
        self._tooltip_page.set_default_timeout(30000)
        self._page_load_time = time.time()

    async def _create_ptb_page(self):
        """Create or recreate the PTB page for price_to_beat."""
        logger.info("Creating PTB page...")
        if self._ptb_page:
            await self._ptb_page.close()

        self._ptb_page = await self._context.new_page()
        self._ptb_page.set_default_timeout(30000)

    async def _navigate_tooltip_page(self):
        """Navigate tooltip page to current market and setup monitoring."""
        current_epoch = get_current_epoch()
        url = get_url(current_epoch)

        logger.info(f"Tooltip page: navigating to {url}")

        self._current_epoch = current_epoch
        self._tooltip_page_url = url
        await self._tooltip_page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await self._tooltip_page.wait_for_timeout(4000)

        self._page_load_time = time.time()

        await self._setup_tooltip_monitoring()
        await self._hover_to_trigger_tooltip()
        await self._tooltip_page.wait_for_timeout(500)
        await self._hover_to_trigger_tooltip()

    async def _setup_tooltip_monitoring(self):
        """Setup polling to detect current_price and chart_timestamp changes."""
        await self._tooltip_page.evaluate(f"""() => {{
            window.tooltipData = {{ price: null, timestamp: null, changed: false }};
            window.__tooltip_selectors = {SELECTORS["current_price"] + SELECTORS["timestamp"]};

            const selectors = {{
                currentPrice: {SELECTORS["current_price"]},
                timestamp: {SELECTORS["timestamp"]}
            }};

            const getElementText = (selectors) => {{
                for (const sel of selectors) {{
                    const el = document.querySelector(sel);
                    if (el) return el.innerText.trim();
                }}
                return null;
            }};

            const checkTooltip = () => {{
                const price = getElementText(selectors.currentPrice);
                const ts = getElementText(selectors.timestamp);

                if (price !== null && price !== window.tooltipData.price) {{
                    window.tooltipData.price = price;
                    window.tooltipData.changed = true;
                }}
                if (ts !== null && ts !== window.tooltipData.timestamp) {{
                    window.tooltipData.timestamp = ts;
                    window.tooltipData.changed = true;
                }}
            }};

            setInterval(checkTooltip, {self._check_interval_ms});
        }}""")

    async def _hover_to_trigger_tooltip(self):
        """Hover at oscillating x positions to trigger tooltip."""
        hover_info = await self._tooltip_page.evaluate(
            f"""(xPercent) => {{
                const container = document.querySelector('#price-chart-container');
                if (!container) return null;
                
                const canvas = container.querySelector('canvas');
                let rect;
                
                if (canvas) {{
                    rect = canvas.getBoundingClientRect();
                }} else {{
                    rect = container.getBoundingClientRect();
                }}
                
                if (rect) {{
                    return {{
                        x: rect.left + rect.width * xPercent,
                        y: rect.top + rect.height * 0.2,
                        xPercent: xPercent
                    }};
                }}
                
                return null;
            }}""",
            self._hover_x_percent,
        )

        if not hover_info:
            logger.warning("Could not determine hover position")
            return

        await self._tooltip_page.mouse.move(hover_info["x"], hover_info["y"])
        await self._tooltip_page.wait_for_timeout(100)
        self._last_hover_time = time.time()
        logger.log(
            TRACE,
            f"Hovered at {hover_info['xPercent'] * 100:.1f}%: ({hover_info['x']:.0f}, {hover_info['y']:.0f})",
        )

    async def _maybe_rehover(self):
        """Re-hover every 0.5 seconds."""
        elapsed = time.time() - self._last_hover_time
        if elapsed > 0.5:
            if self._hover_x_percent > 0.885:
                self._hover_x_percent = 0.8849
            else:
                self._hover_x_percent = 0.8851

            logger.log(TRACE, f"Re-hovering at {self._hover_x_percent * 100:.2f}%")
            await self._hover_to_trigger_tooltip()

    async def _navigate_ptb_page(self, epoch: int):
        """Navigate PTB page to specific epoch and wait for price_to_beat."""
        url = get_url(epoch)
        logger.info(f"PTB page: navigating to {url}")

        await self._ptb_page.goto(url, wait_until="domcontentloaded", timeout=60000)

        # Wait for price_to_beat to appear (can take several seconds)
        max_wait = 15
        start_wait = time.time()

        while time.time() - start_wait < max_wait:
            ptb_value = await self._extract_ptb_from_page()
            # PTB must be > 0 to be valid (initial load shows 0)
            if ptb_value is not None and ptb_value > 0:
                self._price_to_beat = ptb_value
                self._waiting_for_ptb = False
                logger.info(f"Got price_to_beat for epoch {epoch}: {ptb_value}")
                return True

            await self._ptb_page.wait_for_timeout(1000)

        logger.warning(f"Timeout waiting for price_to_beat for epoch {epoch}")
        return False

    async def _extract_ptb_from_page(self) -> Optional[float]:
        """Extract price_to_beat from PTB page."""
        for selector in SELECTORS["price_to_beat"]:
            try:
                elem = await self._ptb_page.query_selector(selector)
                if elem:
                    text = await elem.inner_text()
                    if text:
                        value = float(text.replace("$", "").replace(",", ""))
                        return value
            except Exception:
                continue
        return None

    async def _check_market_transition(self):
        """Check if session changed and handle PTB page transition (non-blocking)."""
        current_epoch = get_current_epoch()

        if current_epoch != self._current_epoch:
            logger.info(f"Session transition: {self._current_epoch} -> {current_epoch}")

            # Set fallback PTB using last current_price
            if self._last_current_price is not None:
                logger.info(f"Using fallback PTB: {self._last_current_price}")

            self._waiting_for_ptb = True
            self._waiting_for_ptb_start = time.time()
            self._ptb_pending_epoch = current_epoch

            # Start PTB page navigation in background (non-blocking within same event loop)
            asyncio.create_task(self._navigate_ptb_page_async(current_epoch))

            self._current_epoch = current_epoch
            return True

        # Check if PTB is ready (non-blocking)
        await self._check_ptb_ready()

        return False

    async def _navigate_ptb_page_async(self, epoch: int):
        """Navigate PTB page asynchronously (runs in background of main event loop)."""
        url = get_url(epoch)
        logger.info(f"PTB async: navigating to {url}")

        try:
            # Start navigation but don't wait for it to complete
            nav_task = asyncio.create_task(
                self._ptb_page.goto(url, wait_until="domcontentloaded", timeout=60000)
            )

            # Wait for navigation to complete
            await nav_task

            # Now poll for PTB
            max_wait = 15
            start_wait = time.time()

            while time.time() - start_wait < max_wait:
                ptb_value = await self._extract_ptb_from_page()
                # PTB must be > 0 to be valid
                if ptb_value is not None and ptb_value > 0:
                    self._price_to_beat = ptb_value
                    self._waiting_for_ptb = False
                    logger.info(
                        f"PTB async: got price_to_beat for epoch {epoch}: {ptb_value}"
                    )
                    return True

                await self._ptb_page.wait_for_timeout(1000)

            logger.warning(
                f"PTB async: timeout waiting for price_to_beat for epoch {epoch}"
            )
        except Exception as e:
            logger.error(f"PTB async: error: {e}")

        self._waiting_for_ptb = False
        return False

        try:
            await self._ptb_page.goto(url, wait_until="domcontentloaded", timeout=60000)

            max_wait = 15
            start_wait = time.time()

            while time.time() - start_wait < max_wait:
                ptb_value = await self._extract_ptb_from_page()
                # PTB must be > 0 to be valid
                if ptb_value is not None and ptb_value > 0:
                    self._price_to_beat = ptb_value
                    self._waiting_for_ptb = False
                    logger.info(
                        f"PTB background: got price_to_beat for epoch {epoch}: {ptb_value}"
                    )
                    return True

                await self._ptb_page.wait_for_timeout(1000)

            logger.warning(
                f"PTB background: timeout waiting for price_to_beat for epoch {epoch}"
            )
        except Exception as e:
            logger.error(f"PTB background: error: {e}")

        self._waiting_for_ptb = False
        return False

    async def _check_ptb_ready(self):
        """Non-blocking check if PTB is available."""
        if self._waiting_for_ptb and self._ptb_pending_epoch == self._current_epoch:
            ptb_value = await self._extract_ptb_from_page()
            if ptb_value is not None and ptb_value > 0:
                self._price_to_beat = ptb_value
                self._waiting_for_ptb = False
                logger.info(f"PTB ready: {ptb_value}")

    async def _check_stale_and_reload(self):
        """Check if tooltip data is stale and reload if needed."""
        # Skip for first 10 seconds after page load
        if time.time() - self._page_load_time < 10:
            return

        # Check if we have data
        if self._last_current_price is None:
            return

        # Check staleness
        time_since_change = time.time() - self._last_data_change_time
        if time_since_change > self._stale_threshold:
            logger.warning(
                f"Tooltip data stale ({time_since_change:.1f}s), reloading page"
            )
            await self._reload_tooltip_page()

    async def _reload_tooltip_page(self):
        """Reload the tooltip page to current market."""
        if self._is_recovering:
            logger.debug("Reload already in progress")
            return

        self._is_recovering = True
        try:
            await self._navigate_tooltip_page()
            logger.info("Tooltip page reloaded")
        finally:
            self._is_recovering = False

    async def _extract_tooltip_data(self) -> tuple:
        """Extract current_price and chart_timestamp from tooltip page."""
        try:
            data = await self._tooltip_page.evaluate("() => window.tooltipData")
            if data and data.get("changed"):
                await self._tooltip_page.evaluate(
                    "() => window.tooltipData.changed = false"
                )
                return data.get("price"), data.get("timestamp")
        except Exception as e:
            logger.debug(f"Error extracting tooltip data: {e}")
        return None, None

    async def monitor(self, duration: int = None):
        await self._init_browser()

        # Create both pages
        await self._create_tooltip_page()
        await asyncio.sleep(1)
        await self._create_ptb_page()

        # Initial navigation
        await self._navigate_tooltip_page()
        await self._navigate_ptb_page(get_current_epoch())

        start_time = asyncio.get_event_loop().time()

        logger.info(f"Monitoring Polymarket BTC 5min markets... (Ctrl+C to stop)")

        try:
            while True:
                # Check for external reload request
                if self._reload_event.is_set():
                    self._reload_event.clear()
                    await self._reload_tooltip_page()

                # Check session transition
                await self._check_market_transition()

                # Re-hover to keep tooltip active
                await self._maybe_rehover()

                # Check staleness
                await self._check_stale_and_reload()

                # Extract data from tooltip page
                price_str, chart_ts = await self._extract_tooltip_data()

                if price_str:
                    try:
                        price = float(price_str.replace("$", "").replace(",", ""))
                    except (ValueError, AttributeError):
                        price = None

                    if price is not None:
                        # Update last values
                        self._last_current_price = price
                        self._last_chart_timestamp = chart_ts or ""
                        self._last_data_change_time = time.time()

                        # Determine price_to_beat
                        if self._waiting_for_ptb and self._price_to_beat is None:
                            # Still waiting for official PTB, use fallback
                            ptb = self._last_current_price
                            wait_time = time.time() - self._waiting_for_ptb_start
                            logger.debug(
                                f"[waiting_for_ptb] {wait_time:.1f}s elapsed, using fallback PTB: {ptb}"
                            )
                        else:
                            ptb = self._price_to_beat

                        # Notify callbacks
                        self._notify_callbacks(price, ptb, chart_ts or "")

                        logger.debug(f"[chart:{chart_ts}] price: {price} | ptb: {ptb}")

                if duration:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed >= duration:
                        break

                await asyncio.sleep(0.5)

        finally:
            await self.close()

    async def close(self):
        if self._tooltip_page:
            await self._tooltip_page.close()
            self._tooltip_page = None
        if self._ptb_page:
            await self._ptb_page.close()
            self._ptb_page = None
        if self._context:
            await self._context.close()
            self._context = None
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    def is_recovering(self) -> bool:
        return self._is_recovering

    def trigger_reload(self):
        """Trigger reload from external thread (main.py)."""
        logger.info("External reload requested")
        self._reload_event.set()


async def run_polymarket_stream(duration: int = None):
    scraper = PolymarketStreamScraper()
    await scraper.monitor(duration=duration)
