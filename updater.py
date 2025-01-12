import os
import json
import gspread
import asyncio
import aiohttp
from google.oauth2.service_account import Credentials
from tradingview_ta import TA_Handler, Interval
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional

# Existing credential setup code remains the same
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

# Connection pool settings
MAX_CONNECTIONS = 100
CONCURRENT_REQUESTS = 20
RATE_LIMIT = 50  # requests per second

class RateLimiter:
    def __init__(self, rate_limit):
        self.rate_limit = rate_limit
        self.tokens = rate_limit
        self.last_update = time.time()
        self.lock = asyncio.Lock()
    
    async def acquire(self):
        async with self.lock:
            now = time.time()
            time_passed = now - self.last_update
            self.tokens = min(self.rate_limit, self.tokens + time_passed * self.rate_limit)
            self.last_update = now
            
            if self.tokens < 1:
                wait_time = (1 - self.tokens) / self.rate_limit
                await asyncio.sleep(wait_time)
                self.tokens = 0
            else:
                self.tokens -= 1

def handle_null_value(value):
    if value is None or value == "null":
        return "-"
    try:
        return round(float(value), 2)
    except ValueError:
        return value

class StockAnalyzer:
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: RateLimiter):
        self.session = session
        self.rate_limiter = rate_limiter
        self.cache: Dict[str, Dict] = {}
        
    async def get_stock_analysis(self, ticker: str, interval: Interval) -> Optional[TA_Handler]:
        cache_key = f"{ticker}_{interval}"
        
        if cache_key in self.cache and (time.time() - self.cache[cache_key]['timestamp']) < 3600:
            return self.cache[cache_key]['data']
        
        await self.rate_limiter.acquire()
        
        exchanges = ["NASDAQ", "NYSE", "AMEX"]
        for exchange in exchanges:
            try:
                handler = TA_Handler(
                    symbol=ticker,
                    exchange=exchange,
                    screener="america",
                    interval=interval,
                    session=self.session
                )
                analysis = await asyncio.to_thread(handler.get_analysis)
                self.cache[cache_key] = {'data': analysis, 'timestamp': time.time()}
                return analysis
            except Exception:
                continue
        return None

async def process_ticker(analyzer: StockAnalyzer, ticker: str) -> List:
    # Get EMA 200 with 5-minute interval
    ema_analysis = await analyzer.get_stock_analysis(ticker, Interval.INTERVAL_5_MINUTES)
    ema_200_5min = handle_null_value(ema_analysis.indicators.get("EMA200", "null")) if ema_analysis else "-"

    # Get daily interval indicators
    analysis = await analyzer.get_stock_analysis(ticker, Interval.INTERVAL_1_DAY)
    if analysis:
        stoch_rsi_fast = handle_null_value(analysis.indicators.get("Stoch.RSI.K", "null"))
        macd_level = handle_null_value(analysis.indicators.get("MACD.macd", "null"))
        macd_signal = handle_null_value(analysis.indicators.get("MACD.signal", "null"))
        williams_r = handle_null_value(analysis.indicators.get("W.R", "null"))
        bbpower = handle_null_value(analysis.indicators.get("BBPower", "null"))
        ema_200_daily = handle_null_value(analysis.indicators.get("EMA200", "null"))
        bb_upper = handle_null_value(analysis.indicators.get("BB.upper", "null"))
        bb_lower = handle_null_value(analysis.indicators.get("BB.lower", "null"))
        open_value = handle_null_value(analysis.indicators.get("open", "null"))
        stochk = handle_null_value(analysis.indicators.get("Stoch.K", "null"))
        mom = handle_null_value(analysis.indicators.get("Mom", "null"))
    else:
        stoch_rsi_fast = macd_level = macd_signal = williams_r = bbpower = ema_200_daily = bb_upper = bb_lower = open_value = stochk = mom = "-"

    # Get 4-hour indicators
    ema_200_4h_analysis = await analyzer.get_stock_analysis(ticker, Interval.INTERVAL_4_HOURS)
    ema_200_4h = handle_null_value(ema_200_4h_analysis.indicators.get("EMA200", "null")) if ema_200_4h_analysis else "-"
    williams_r4h = handle_null_value(ema_200_4h_analysis.indicators.get("W.R", "null")) if ema_200_4h_analysis else "-"
    stochk4h = handle_null_value(ema_200_4h_analysis.indicators.get("Stoch.K", "null")) if ema_200_4h_analysis else "-"

    # Get 1-hour indicators
    ema_200_1h_analysis = await analyzer.get_stock_analysis(ticker, Interval.INTERVAL_1_HOUR)
    ema_200_1h = handle_null_value(ema_200_1h_analysis.indicators.get("EMA200", "null")) if ema_200_1h_analysis else "-"
    williams_r1h = handle_null_value(ema_200_1h_analysis.indicators.get("W.R", "null")) if ema_200_1h_analysis else "-"
    stochk1h = handle_null_value(ema_200_1h_analysis.indicators.get("Stoch.K", "null")) if ema_200_1h_analysis else "-"

    # Get 15-minute indicators
    williams_r15m_analysis = await analyzer.get_stock_analysis(ticker, Interval.INTERVAL_15_MINUTES)
    williams_r15m = handle_null_value(williams_r15m_analysis.indicators.get("W.R", "null")) if williams_r15m_analysis else "-"
    stochk15min = handle_null_value(williams_r15m_analysis.indicators.get("Stoch.K", "null")) if williams_r15m_analysis else "-"

    # Get weekly indicators
    williams_r_week_analysis = await analyzer.get_stock_analysis(ticker, Interval.INTERVAL_1_WEEK)
    if williams_r_week_analysis:
        williams_r_week = handle_null_value(williams_r_week_analysis.indicators.get("W.R", "null"))
        stoch_rsi_fast_1week = handle_null_value(williams_r_week_analysis.indicators.get("Stoch.RSI.K", "null"))
    else:
        williams_r_week = stoch_rsi_fast_1week = "-"

    # Return the values in the exact same order as the original code
    return [None, None, None, None, ema_200_5min, None, ema_200_daily, None, ema_200_4h, None, 
            ema_200_1h, None, bb_upper, None, bb_lower, None, None, None, None, None, None, None,
            open_value, stoch_rsi_fast, williams_r, None, None, bbpower, None, None, None, 
            macd_level, macd_signal, mom, None, None, williams_r_week, stoch_rsi_fast_1week,
            None, None, None, None, None, stochk, None, None, None, None, williams_r4h,
            stochk4h, None, None, None, None, williams_r1h, stochk1h, None, None, None, None,
            williams_r15m, stochk15min]

async def update_stock_prices_async(sheet, tickers: List[str], batch_size: int = 200):
    connector = aiohttp.TCPConnector(limit=MAX_CONNECTIONS)
    rate_limiter = RateLimiter(RATE_LIMIT)
    
    async with aiohttp.ClientSession(connector=connector) as session:
        analyzer = StockAnalyzer(session, rate_limiter)
        
        for i in range(0, len(tickers), batch_size):
            batch_tickers = tickers[i:i + batch_size]
            tasks = [process_ticker(analyzer, ticker) for ticker in batch_tickers]
            
            # Process tickers concurrently with controlled concurrency
            results = []
            for batch in range(0, len(tasks), CONCURRENT_REQUESTS):
                batch_tasks = tasks[batch:batch + CONCURRENT_REQUESTS]
                batch_results = await asyncio.gather(*batch_tasks)
                results.extend(batch_results)
            
            # Update Google Sheet
            cell_range = f'A{i + 3}:BJ{i + 2 + len(results)}'
            sheet.update(cell_range, results)
            
            print(f"Updated stocks from {i + 1} to {i + len(batch_tickers)}")

def update_stock_prices():
    # Get the sheet and tickers
    creds = Credentials.from_service_account_info(json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON")), scopes=SCOPE)
    client = gspread.authorize(creds)
    sheet = client.open('Flux Capacitor').worksheet("Live Raw Data API + Scraping + GOOGLEFINANCE")
    tickers = sheet.col_values(1)[2:]  # Starting from the 3rd row
    
    # Run the async update function
    asyncio.run(update_stock_prices_async(sheet, tickers, batch_size=200))

def update_stock_prices_and_schedule():
    global is_updating
    if not is_updating:
        is_updating = True
        try:
            update_stock_prices()
        finally:
            is_updating = False
    update_stock_prices_and_schedule()

if __name__ == "__main__":
    is_updating = False
    update_stock_prices_and_schedule()
