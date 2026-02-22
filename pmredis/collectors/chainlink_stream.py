import asyncio
import logging
from datetime import datetime

from playwright.async_api import (
    async_playwright,
    TimeoutError as PlaywrightTimeoutError,
)

logger = logging.getLogger(__name__)


class ChainlinkStreamScraper:
    def __init__(self, network: str = "ethereum"):
        self.network = network
        self._browser = None
        self._page = None
        self._playwright = None

    async def _init_browser(self):
        if self._playwright is None:
            self._playwright = await async_playwright().start()
            self._browser = await self._playwright.chromium.launch(headless=True)
            self._page = await self._browser.new_page()

    async def _hover_chart_and_get_tooltip(self):
        try:
            # Find the chart container and hover on right edge
            hover_info = await self._page.evaluate("""() => {
                const containers = document.querySelectorAll('[class*="LiveAnswerGraph"]');
                for (const c of containers) {
                    const rect = c.getBoundingClientRect();
                    if (rect.width > 300) {
                        return {x: rect.right - 50, y: rect.top + rect.height / 2};
                    }
                }
                return null;
            }""")

            if hover_info:
                await self._page.mouse.move(hover_info["x"], hover_info["y"])
                await self._page.wait_for_timeout(200)

            # Extract tooltip data
            tooltip_data = await self._page.evaluate("""() => {
                const tooltip = document.querySelector('.recharts-tooltip-wrapper');
                if (!tooltip || tooltip.style.visibility !== 'visible') return null;
                
                const html = tooltip.innerHTML;
                
                // Extract prices - look for "Label: </span><span>$PRICE"
                const bidMatch = html.match(/Bid:?\s*<\\/span><span[^>]*>\\$?([0-9,]+\\.?\\d*)/);
                const midMatch = html.match(/Mid-price:?\s*<\\/span><span[^>]*>\\$?([0-9,]+\\.?\\d*)/);
                const askMatch = html.match(/Ask:?\s*<\\/span><span[^>]*>\\$?([0-9,]+\\.?\\d*)/);
                
                // Extract time from the title and convert to 24-hour format
                const timeMatch = html.match(/>(\d{1,2}:\d{2}:\d{2}\s*(?:AM|PM))/i);
                let time24 = '';
                if (timeMatch) {
                    const time12 = timeMatch[1].trim();
                    const [time, period] = time12.split(/(?=\s*[AP]M)/i);
                    const [h, m, s] = time.trim().split(':');
                    let hour = parseInt(h, 10);
                    if (period.toUpperCase().includes('PM') && hour !== 12) hour += 12;
                    if (period.toUpperCase().includes('AM') && hour === 12) hour = 0;
                    time24 = `${hour.toString().padStart(2, '0')}:${m}:${s}`;
                }
                
                return {
                    time: time24,
                    bid: bidMatch ? '$' + bidMatch[1] : '',
                    mid: midMatch ? '$' + midMatch[1] : '',
                    ask: askMatch ? '$' + askMatch[1] : ''
                };
            }""")

            return tooltip_data

        except Exception as e:
            return None

    async def _setup_mutation_observer(self):
        await self._page.evaluate("""
            () => {
                window.priceChanged = false;
                window.lastMid = '';
                window.lastBid = '';
                window.lastAsk = '';
                window.lastTooltipTime = '';
                
                // Target only the price display area instead of entire body
                const priceContainer = document.querySelector('[class*="LiveAnswerGraph"]') || 
                                       document.querySelector('.price-display') ||
                                       document.querySelector('[class*="price"]');
                
                if (!priceContainer) return;
                
                const parsePrice = (text, label) => {
                    const idx = text.indexOf(label);
                    if (idx >= 0) {
                        const after = text.substring(idx, idx + 40);
                        const match = after.match(/\\$?([0-9,]+\\.?[0-9]*)/);
                        return match ? '$' + match[1] : null;
                    }
                    return null;
                };
                
                const checkPrices = () => {
                    const text = priceContainer.innerText;
                    const mid = parsePrice(text, 'Mid-price') || parsePrice(text, 'Mid');
                    const bid = parsePrice(text, 'Bid price') || parsePrice(text, 'Bid');
                    const ask = parsePrice(text, 'Ask price') || parsePrice(text, 'Ask');
                    
                    if (mid && (mid !== window.lastMid || bid !== window.lastBid || ask !== window.lastAsk)) {
                        window.lastMid = mid;
                        window.lastBid = bid;
                        window.lastAsk = ask;
                        window.priceChanged = true;
                    }
                };
                
                const observer = new MutationObserver((mutations) => {
                    checkPrices();
                });
                
                observer.observe(priceContainer, {
                    childList: true,
                    subtree: true,
                    characterData: true
                });
                
                // Also check periodically (every 2s instead of very frequent)
                setInterval(checkPrices, 2000);
            }
        """)

    async def monitor(self, duration: int = None):
        await self._init_browser()

        url = "https://data.chain.link/streams/btc-usd-cexprice-streams"
        await self._page.goto(url, wait_until="networkidle", timeout=60000)
        await self._page.wait_for_timeout(3000)

        # Initial hover to position mouse at right edge
        await self._hover_chart_and_get_tooltip()

        await self._setup_mutation_observer()

        start_time = asyncio.get_event_loop().time()
        last_tooltip_data = None

        logger.info(
            f"Monitoring BTC/USD CEX Price Stream on Chainlink... (Ctrl+C to stop)"
        )

        try:
            while True:
                changed = await self._page.evaluate("() => window.priceChanged")

                if changed:
                    mid = await self._page.evaluate("() => window.lastMid")
                    bid = await self._page.evaluate("() => window.lastBid")
                    ask = await self._page.evaluate("() => window.lastAsk")

                    # Get tooltip data (hover and extract)
                    tooltip = await self._hover_chart_and_get_tooltip()

                    local_ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]

                    if tooltip and tooltip.get("mid"):
                        chart_time = tooltip.get("time", "N/A")
                        print(
                            f"[{local_ts}] [chart:{chart_time}] BTC/USD → Mid: {tooltip['mid']} | Bid: {tooltip['bid']} | Ask: {tooltip['ask']}"
                        )
                    else:
                        print(
                            f"[{local_ts}] [chart:N/A] BTC/USD → Mid: {mid} | Bid: {bid} | Ask: {ask}"
                        )

                    await self._page.evaluate("() => { window.priceChanged = false; }")

                if duration:
                    elapsed = asyncio.get_event_loop().time() - start_time
                    if elapsed >= duration:
                        break

                await asyncio.sleep(1)

        finally:
            await self.close()

    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()


async def run_chainlink_stream(network: str = "ethereum", duration: int = None):
    scraper = ChainlinkStreamScraper(network=network)
    await scraper.monitor(duration=duration)
