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

# [Previous SCOPE and settings code remains the same]

class StockAnalyzer:
    def __init__(self):
        self.cache: Dict[str, Dict] = {}
        
    def get_stock_analysis(self, ticker: str, interval: Interval) -> Optional[TA_Handler]:
        cache_key = f"{ticker}_{interval}"
        
        if cache_key in self.cache and (time.time() - self.cache[cache_key]['timestamp']) < 3600:
            return self.cache[cache_key]['data']
        
        exchanges = ["NASDAQ", "NYSE", "AMEX"]
        last_exception = None
        
        for exchange in exchanges:
            max_retries = 3
            retry_delay = 2  # seconds
            
            for attempt in range(max_retries):
                try:
                    handler = TA_Handler(
                        symbol=ticker,
                        exchange=exchange,
                        screener="america",
                        interval=interval
                    )
                    analysis = handler.get_analysis()
                    
                    # Verify that we actually got data
                    if analysis and hasattr(analysis, 'indicators') and analysis.indicators:
                        # Check if any key indicators are present
                        key_indicators = ["EMA200", "Stoch.RSI.K", "W.R", "MACD.macd"]
                        if any(indicator in analysis.indicators for indicator in key_indicators):
                            self.cache[cache_key] = {'data': analysis, 'timestamp': time.time()}
                            return analysis
                    
                    # If we didn't get valid data, raise an exception to trigger retry
                    raise Exception("Invalid or empty analysis data received")
                    
                except Exception as e:
                    last_exception = e
                    if attempt < max_retries - 1:  # Don't sleep on the last attempt
                        time.sleep(retry_delay)
                        retry_delay *= 2  # Exponential backoff
                    continue
                
        print(f"Failed to get data for {ticker} on interval {interval} after all retries. Last error: {last_exception}")
        return None

async def process_ticker(analyzer: StockAnalyzer, ticker: str) -> List:
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor() as pool:
        try:
            print(f"Processing {ticker}...")
            
            # Get EMA 200 with 5-minute interval
            ema_analysis = await loop.run_in_executor(pool, analyzer.get_stock_analysis, ticker, Interval.INTERVAL_5_MINUTES)
            ema_200_5min = handle_null_value(ema_analysis.indicators.get("EMA200", "null")) if ema_analysis else "-"

            # Add small delay between requests for the same ticker
            await asyncio.sleep(0.5)

            # Get daily interval indicators
            analysis = await loop.run_in_executor(pool, analyzer.get_stock_analysis, ticker, Interval.INTERVAL_1_DAY)
            if analysis and hasattr(analysis, 'indicators'):
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

            await asyncio.sleep(0.5)

            # Get 4-hour indicators
            ema_200_4h_analysis = await loop.run_in_executor(pool, analyzer.get_stock_analysis, ticker, Interval.INTERVAL_4_HOURS)
            ema_200_4h = handle_null_value(ema_200_4h_analysis.indicators.get("EMA200", "null")) if ema_200_4h_analysis else "-"
            williams_r4h = handle_null_value(ema_200_4h_analysis.indicators.get("W.R", "null")) if ema_200_4h_analysis else "-"
            stochk4h = handle_null_value(ema_200_4h_analysis.indicators.get("Stoch.K", "null")) if ema_200_4h_analysis else "-"

            await asyncio.sleep(0.5)

            # [Rest of the indicator fetching code remains the same]

            print(f"Successfully processed {ticker}")
            
            # Return the values in the exact same order as before
            return [None, None, None, None, ema_200_5min, None, ema_200_daily, None, ema_200_4h, None, 
                    ema_200_1h, None, bb_upper, None, bb_lower, None, None, None, None, None, None, None,
                    open_value, stoch_rsi_fast, williams_r, None, None, bbpower, None, None, None, 
                    macd_level, macd_signal, mom, None, None, williams_r_week, stoch_rsi_fast_1week,
                    None, None, None, None, None, stochk, None, None, None, None, williams_r4h,
                    stochk4h, None, None, None, None, williams_r1h, stochk1h, None, None, None, None,
                    williams_r15m, stochk15min]
                    
        except Exception as e:
            print(f"Error processing {ticker}: {str(e)}")
            # Return a row of "-" values in case of complete failure
            return [None] * 4 + ["-"] + [None] * 1 + ["-"] * 17  # Adjust the number of "-" based on your columns

# [Rest of the code remains the same]
