import asyncio
import logging
import time
import threading
from datetime import datetime
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
    def __init__(self, check_interval_ms: int = 250):
        self._browser = None
        self._page = None  # Main page for tooltip (current_price, chart_timestamp)
        self._ptb_page = None  # Temp page for price_to_beat on session switch
        self._playwright = None
        self._current_epoch = None
        self._current_price_to_beat: Optional[float] = None  # Latest price_to_beat
        self._last_hover_time = 0
        self._hover_x_percent = 0.885
        self._last_data_time = 0
        self._stall_recovery_attempts = 0
        self._consecutive_no_data = 0
        self._check_interval_ms = check_interval_ms
        self._callbacks = []
        self._is_recovering = False  # Flag to prevent duplicate recovery
        self._reload_event = (
            threading.Event()
        )  # Event to signal reload from main thread

    def add_callback(self, callback):
        """Add a callback for price updates."""
        self._callbacks.append(callback)

    def _notify_callbacks(self, price, price_to_beat, timestamp):
        """Notify all callbacks with new price data."""
        for callback in self._callbacks:
            try:
                callback(price, price_to_beat, timestamp)
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
            self._page = await self._browser.new_page(
                viewport={"width": 1280, "height": 720},
                user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            )
            self._page.set_default_timeout(30000)

    def _try_selectors(self, selectors: list) -> str:
        """Try multiple selectors and return the first one that works."""
        return None

    async def _validate_tooltip_data(self) -> bool:
        """Check if tooltip data is actually present after hover."""
        try:
            data = await self._page.evaluate("""() => {
                const selectors = window.__selector_list || [];
                for (const sel of selectors) {
                    const el = document.querySelector(sel);
                    if (el && el.innerText.trim()) {
                        return true;
                    }
                }
                return false;
            }""")
            return data
        except:
            return False

    async def _hover_at_position(self, x: float, y: float, label: str) -> bool:
        """Hover at a specific position and return success status."""
        try:
            await self._page.mouse.move(x, y)
            await self._page.wait_for_timeout(500)
            self._last_hover_time = time.time()
            logger.log(TRACE, f"Hovered at {label}: ({x}, {y})")
            return True
        except Exception as e:
            logger.warning(f"Hover at {label} failed: {e}")
            return False

    async def _hover_to_trigger_tooltip(self):
        """Hover at oscillating x positions (88.4% ↔ 88.6%) to trigger tooltip."""
        hover_info = await self._page.evaluate(
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
            logger.warning("Could not determine hover position - container not found")
            return

        await self._page.mouse.move(hover_info["x"], hover_info["y"])
        await self._page.wait_for_timeout(100)
        self._last_hover_time = time.time()
        logger.log(
            TRACE,
            f"Hovered at {hover_info['xPercent'] * 100:.1f}%: ({hover_info['x']:.0f}, {hover_info['y']:.0f})",
        )

    async def _navigate_to_current_market(self):
        self._current_epoch = get_current_epoch()
        url = get_url(self._current_epoch)
        logger.info(f"Navigating to: {url}")
        self._last_data_time = time.time()  # Start timer BEFORE navigation
        await self._page.goto(url, wait_until="domcontentloaded", timeout=60000)
        await self._page.wait_for_timeout(4000)

        # Reset stall detection counters
        self._consecutive_no_data = 0
        self._stall_recovery_attempts = 0

        # Setup observer FIRST, then hover
        await self._setup_mutation_observer()
        await self._hover_to_trigger_tooltip()
        # Additional hover after a short delay to ensure tooltip appears
        await self._page.wait_for_timeout(500)
        await self._hover_to_trigger_tooltip()

    async def _setup_mutation_observer(self):
        """Setup MutationObserver to detect price changes with robust selector handling."""
        # Flatten selectors for JS
        all_selectors = []
        for key in ["price_to_beat", "current_price", "timestamp"]:
            all_selectors.extend(SELECTORS.get(key, []))

        await self._page.evaluate(f"""() => {{
            window.priceChanged = false;
            window.lastPriceToBeat = '';
            window.lastCurrentPrice = '';
            window.lastTimestamp = '';
            window.currentData = null;
            window.__selector_list = {all_selectors};

            // Try each selector set until we find one that works
            const selectorSets = {{
                priceToBeat: {SELECTORS["price_to_beat"]},
                currentPrice: {SELECTORS["current_price"]},
                timestamp: {SELECTORS["timestamp"]}
            }};

            const getActiveSelectors = () => {{
                const result = {{}};
                for (const [key, selectors] of Object.entries(selectorSets)) {{
                    for (const sel of selectors) {{
                        const el = document.querySelector(sel);
                        if (el) {{
                            result[key] = sel;
                            break;
                        }}
                    }}
                }}
                return result;
            }};

            const extractData = () => {{
                const result = {{}};
                const activeSelectors = getActiveSelectors();
                
                if (activeSelectors.priceToBeat) {{
                    const ptbEl = document.querySelector(activeSelectors.priceToBeat);
                    if (ptbEl) {{
                        result.priceToBeat = ptbEl.innerText.trim();
                    }}
                }}
                
                if (activeSelectors.currentPrice) {{
                    const priceEl = document.querySelector(activeSelectors.currentPrice);
                    if (priceEl) {{
                        result.currentPrice = priceEl.innerText.trim();
                    }}
                }}
                
                if (activeSelectors.timestamp) {{
                    const timeEl = document.querySelector(activeSelectors.timestamp);
                    if (timeEl) {{
                        result.timestamp = timeEl.innerText.trim();
                    }}
                }}
                
                // Debug: log if selectors are missing
                if (!result.priceToBeat || !result.currentPrice || !result.timestamp) {{
                    console.log('[PM Stream] Missing data - ptb:', !!result.priceToBeat, 'price:', !!result.currentPrice, 'time:', !!result.timestamp);
                }}
                
                return result;
            }};

            const checkChanges = () => {{
                const data = extractData();
                
                if (data.currentPrice !== undefined && data.currentPrice !== null && 
                    data.priceToBeat !== undefined && data.priceToBeat !== null && 
                    data.timestamp !== undefined && data.timestamp !== null) {{
                    if (data.priceToBeat !== window.lastPriceToBeat || 
                        data.currentPrice !== window.lastCurrentPrice || 
                        data.timestamp !== window.lastTimestamp) {{
                        window.lastPriceToBeat = data.priceToBeat;
                        window.lastCurrentPrice = data.currentPrice;
                        window.lastTimestamp = data.timestamp;
                        window.priceChanged = true;
                        window.currentData = data;
                        console.log('[PM Stream] Data updated:', data.currentPrice, data.priceToBeat, data.timestamp);
                    }}
                }}
            }};

            // Initial check
            setTimeout(checkChanges, 1000);

            // Check at configured interval
            setInterval(checkChanges, {self._check_interval_ms});
        }}""")

    async def _get_price_to_beat_from_page(self, page, epoch: int) -> Optional[float]:
        """Get price_to_beat from a specific page (static DOM, no hover needed)."""
        url = get_url(epoch)
        try:
            await page.goto(url, wait_until="domcontentloaded", timeout=60000)
            await page.wait_for_timeout(3000)

            # Try multiple selectors for price_to_beat
            for selector in SELECTORS["price_to_beat"]:
                try:
                    ptb_elem = await page.query_selector(selector)
                    if ptb_elem:
                        ptb_text = await ptb_elem.inner_text()
                        if ptb_text:
                            ptb_value = float(
                                ptb_text.replace("$", "").replace(",", "")
                            )
                            logger.info(
                                f"Got price_to_beat from epoch {epoch}: {ptb_value}"
                            )
                            return ptb_value
                except Exception:
                    continue

            logger.warning(f"Could not find price_to_beat selector for epoch {epoch}")
            return None
        except Exception as e:
            logger.error(f"Error getting price_to_beat: {e}")
            return None

    async def _check_market_transition(self):
        """Check if we need to get new price_to_beat for new market.

        Uses temp page to get price_to_beat without disrupting main tooltip page.
        """
        current_epoch = get_current_epoch()
        if current_epoch != self._current_epoch:
            logger.info(f"Market transition: {self._current_epoch} -> {current_epoch}")

            # Use temp page to get new price_to_beat
            if self._ptb_page is None:
                self._ptb_page = await self._browser.new_page(
                    viewport={"width": 1280, "height": 720},
                    user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
                )

            # Get price_to_beat from new session
            new_ptb = await self._get_price_to_beat_from_page(
                self._ptb_page, current_epoch
            )
            if new_ptb is not None:
                self._current_price_to_beat = new_ptb
                self._current_epoch = current_epoch
                logger.info(
                    f"Updated price_to_beat to {new_ptb} for epoch {current_epoch}"
                )

            return True
        return False

    async def _check_stall_and_recover(self):
        """Check if stream has stalled and attempt recovery."""
        current_time = time.time()

        # Skip stall detection for first 10 seconds after page load
        time_since_load = current_time - self._last_data_time
        if time_since_load < 10:
            return

        # Check if we have recent data
        has_data = await self._page.evaluate("""() => {
            return window.lastCurrentPrice && window.lastPriceToBeat;
        }""")

        if not has_data:
            self._consecutive_no_data += 1

            if self._consecutive_no_data >= 30:  # ~15 seconds of no data
                self._stall_recovery_attempts += 1

                # Try re-hovering first
                await self._hover_to_trigger_tooltip()

                # If still no data after multiple attempts, re-navigate
                if self._stall_recovery_attempts >= 3:
                    try:
                        await self._navigate_to_current_market()
                    except Exception as e:
                        pass

                    self._stall_recovery_attempts = 0

                self._consecutive_no_data = 0
        else:
            # Reset counters when we have data
            if self._consecutive_no_data > 0:
                logger.debug("Data stream resumed")
            self._consecutive_no_data = 0
            self._stall_recovery_attempts = 0
            self._last_data_time = current_time

    async def _maybe_rehover(self):
        """Re-hover every 0.5 seconds, oscillating x position by 0.01%."""
        elapsed = time.time() - self._last_hover_time
        if elapsed > 0.5:
            if self._hover_x_percent > 0.885:
                self._hover_x_percent = 0.8849
            else:
                self._hover_x_percent = 0.8851

            logger.log(TRACE, f"Re-hovering at {self._hover_x_percent * 100:.2f}%")
            await self._hover_to_trigger_tooltip()

    async def monitor(self, duration: int = None):
        await self._init_browser()
        await self._navigate_to_current_market()

        # Get initial price_to_beat from current session
        self._current_price_to_beat = await self._get_price_to_beat_from_page(
            self._page, self._current_epoch
        )

        start_time = asyncio.get_event_loop().time()

        logger.info(f"Monitoring Polymarket BTC 5min markets... (Ctrl+C to stop)")

        try:
            while True:
                # Check if reload is requested from main thread
                if self._reload_event.is_set():
                    self._reload_event.clear()
                    await self.reload()

                await self._check_market_transition()
                await self._maybe_rehover()
                await self._check_stall_and_recover()

                changed = await self._page.evaluate("() => window.priceChanged")

                if changed:
                    data = await self._page.evaluate("() => window.currentData")

                    if data and data.get("currentPrice"):
                        price_str = data.get("currentPrice", "")
                        chart_ts = data.get("timestamp", "")

                        # Parse price string like "$63,188.58" to float
                        try:
                            price = float(price_str.replace("$", "").replace(",", ""))
                        except (ValueError, AttributeError):
                            price = None

                        # Get price_to_beat from instance variable (set during session switch)
                        price_to_beat = self._current_price_to_beat

                        # Notify callbacks with price from tooltip and price_to_beat from transition
                        if price is not None:
                            self._notify_callbacks(price, price_to_beat, chart_ts)

                        logger.debug(
                            f"[chart:{chart_ts}] price: {price} | price_to_beat: {price_to_beat}"
                        )

                    await self._page.evaluate("() => { window.priceChanged = false; }")

                if duration:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed >= duration:
                        break

                await asyncio.sleep(0.5)

        finally:
            await self.close()

    async def close(self):
        if self._ptb_page:
            await self._ptb_page.close()
            self._ptb_page = None
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    def is_recovering(self) -> bool:
        """Check if recovery is in progress."""
        return self._is_recovering

    def trigger_reload(self):
        """Trigger reload from external thread (main.py)."""
        logger.info("External reload requested")
        self._reload_event.set()

    async def reload(self):
        """Force reload of the main tooltip page (for recovery)."""
        if self._is_recovering:
            logger.debug("Recovery already in progress, skipping")
            return

        self._is_recovering = True
        logger.info("Triggering recovery: re-navigating tooltip page to current market")

        try:
            # Re-navigate the main page to current market
            await self._navigate_to_current_market()

            # Also refresh price_to_beat
            self._current_price_to_beat = await self._get_price_to_beat_from_page(
                self._page, self._current_epoch
            )

            logger.info("Recovery complete")
        finally:
            self._is_recovering = False


async def run_polymarket_stream(duration: int = None):
    scraper = PolymarketStreamScraper()
    await scraper.monitor(duration=duration)
