import os
import json
from flask import Flask, jsonify
import gspread
from google.oauth2.service_account import Credentials
from tradingview_ta import TA_Handler, Interval
import time


app = Flask(__name__)

# Set up Google Sheets API credentials
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

# Load the Google credentials from the environment variable
GOOGLE_CREDENTIALS_JSON = os.getenv("PROJECT_MALAGA_KEY")

if GOOGLE_CREDENTIALS_JSON is None:
    raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable is not set.")

# Create credentials from the JSON string
creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=SCOPE)

# Authorize the Google Sheets client
client = gspread.authorize(creds)

# Open the Google Sheet
SPREADSHEET_NAME = 'Flux Capacitor'
sheet = client.open(SPREADSHEET_NAME).worksheet("Live Raw Data API + Scraping + GOOGLEFINANCE")

# Cache for storing stock analysis results
cache = {}

# Function to get analysis from TradingView for NASDAQ or NYSE
def get_stock_analysis(ticker, interval):
    # Create a unique cache key based on ticker and interval
    cache_key = f"{ticker}_{interval}"

    # Check if the result is already cached
    if cache_key in cache and (time.time() - cache[cache_key]['timestamp']) < 3600:  # Cache for 1 hour
        return cache[cache_key]['data']
    
    exchanges = ["NASDAQ", "NYSE", "AMEX"]
    for exchange in exchanges:
        try:
            # Set up TradingView TA_Handler for the stock ticker and exchange
            handler = TA_Handler(
                symbol=ticker,
                exchange=exchange,
                screener="america",
                interval=interval
            )
            analysis = handler.get_analysis()
            # Cache the results uniquely by ticker and interval
            cache[cache_key] = {'data': analysis, 'timestamp': time.time()}
            return analysis
        except Exception as e:
            # Uncomment the line below if you still want to see error messages in the terminal
            # print(f"Failed to fetch {ticker} data from {exchange}: {e}")
            continue  # Try the next exchange
    return None  # Return None if both exchanges fail

# Function to handle "null" values
def handle_null_value(value):
    if value is None or value == "null":
        return "null"
    try:
        # Round the value to two decimal places if it's a number
        return round(float(value), 2)
    except ValueError:
        return value

def update_stock_prices():
    # Read tickers from column A starting from row 3
    tickers = sheet.col_values(1)[2:]  # Starting from the 3rd row

    batch_size = 50 # Number of tickers to process at a time
    total_tickers = len(tickers)
    stock_data = []

    # Process tickers in batches
    for i in range(0, total_tickers, batch_size):
        batch_tickers = tickers[i:i + batch_size]
        update_values = []  # List to hold updates for batch writing

        for idx, ticker in enumerate(batch_tickers, start=i + 3):  # Row 3 onward
            # Get EMA 200 with 5-minute interval
            ema_analysis = get_stock_analysis(ticker, Interval.INTERVAL_5_MINUTES)
            if ema_analysis is not None:
                ema_200_5min = handle_null_value(ema_analysis.indicators.get("EMA200", "null"))
            else:
                ema_200_5min = "null"

            # Get other indicators with daily interval
            analysis = get_stock_analysis(ticker, Interval.INTERVAL_1_DAY)
            if analysis is not None:
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
                stoch_rsi_fast = macd_level = macd_signal = williams_r = bbpower = ema_200_daily = bb_upper = bb_lower = open_value = stochk = mom = "null"

            # Get EMA 200 with 4-hour and 1-hour intervals
            ema_200_4h_analysis = get_stock_analysis(ticker, Interval.INTERVAL_4_HOURS)
            if ema_200_4h_analysis is not None:
                ema_200_4h = handle_null_value(ema_200_4h_analysis.indicators.get("EMA200", "null"))
            else:
                ema_200_4h = "null"

            williams_r4hanalysis = get_stock_analysis(ticker, Interval.INTERVAL_4_HOURS)
            if williams_r4hanalysis is not None:
                williams_r4h = handle_null_value(williams_r4hanalysis.indicators.get("W.R", "null"))
            else:
                williams_r4h = "null"

            stochk4hanalysis = get_stock_analysis(ticker, Interval.INTERVAL_4_HOURS)
            if stochk4hanalysis is not None:
                stochk4h = handle_null_value(stochk4hanalysis.indicators.get("Stoch.K", "null"))
            else:
                stochk4h = "null"

            ema_200_1h_analysis = get_stock_analysis(ticker, Interval.INTERVAL_1_HOUR)
            if ema_200_1h_analysis is not None:
                ema_200_1h = handle_null_value(ema_200_1h_analysis.indicators.get("EMA200", "null"))
            else:
                ema_200_1h = "null"
            
            williams_r1hanalysis = get_stock_analysis(ticker, Interval.INTERVAL_1_HOUR)
            if williams_r1hanalysis is not None:
                williams_r1h = handle_null_value(williams_r1hanalysis.indicators.get("W.R", "null"))
            else:
                williams_r1h = "null"

            stochk1hanalysis = get_stock_analysis(ticker, Interval.INTERVAL_1_HOUR)
            if stochk1hanalysis is not None:
                stochk1h = handle_null_value(stochk1hanalysis.indicators.get("Stoch.K", "null"))
            else:
                stochk1h = "null"

            williams_r15manalysis = get_stock_analysis(ticker, Interval.INTERVAL_15_MINUTES)
            if williams_r15manalysis is not None:
                williams_r15m = handle_null_value(williams_r15manalysis.indicators.get("W.R", "null"))
            else:
                williams_r15m = "null"

            stochk15minanalysis = get_stock_analysis(ticker, Interval.INTERVAL_15_MINUTES)
            if stochk15minanalysis is not None:
                stochk15min = handle_null_value(stochk15minanalysis.indicators.get("Stoch.K", "null"))
            else:
                stochk15min = "null"

            # Get Williams and Stoch RSI with 1-week interval
            williams_r_week_analysis = get_stock_analysis(ticker, Interval.INTERVAL_1_WEEK)
            if williams_r_week_analysis is not None:
                williams_r_week = handle_null_value(williams_r_week_analysis.indicators.get("W.R", "null"))
                stoch_rsi_fast_1week = handle_null_value(williams_r_week_analysis.indicators.get("Stoch.RSI.K", "null"))
            else:
                williams_r_week = stoch_rsi_fast_1week = "null"

            # Save the results to the list (for API response)
            stock_data.append({
                "ticker": ticker,
                "ema_200_5min": ema_200_5min,
                "stoch_rsi_fast": stoch_rsi_fast,
                "macd_level": macd_level,
                "macd_signal": macd_signal,
                "williams_r": williams_r,
                "bbpower": bbpower,
                "ema_200_daily": ema_200_daily,
                "ema_200_4h": ema_200_4h,
                "ema_200_1h": ema_200_1h,
                "bb_upper": bb_upper,
                "bb_lower": bb_lower,
                "open_value": open_value,
                "williams_r_week": williams_r_week,
                "stoch_rsi_fast_1week": stoch_rsi_fast_1week,
                "stochk": stochk,
                "williams_r4h": williams_r4h,
                "stochk4h": stochk4h,
                "williams_r1h": williams_r1h,
                "stochk1h": stochk1h,
                "williams_r15m": williams_r15m,
                "stochk15min": stochk15min,
                "mom": mom
            })

            # Prepare the update values for batch update
            update_values.append([ema_200_5min, stoch_rsi_fast, williams_r, macd_level, macd_signal, bbpower, ema_200_daily, ema_200_4h, ema_200_1h, bb_upper, bb_lower, open_value, williams_r_week, stoch_rsi_fast_1week, stochk, williams_r4h, stochk4h, williams_r1h, stochk1h, williams_r15m, stochk15min, mom])

        # Update Google Sheet in batch
        if update_values:
            cell_range = f'A{i + 3}:BJ{i + 2 + len(update_values)}'  # Adjust based on columns used
            update_data = []
            for row in update_values:
                # Only updating the relevant columns while keeping the other columns None
                update_data.append([None, None, None, None, row[0], None, row[6], None, row[7], None, row[8], None, row[9], None, row[10], None, None, None, None, None, None, None, row[11], row[1], row[2], None, None, row[5], None, None, None, row[3], row[4], row[21], None, None, row[12], row[13], None, None, None, None, None, row[14], None, None, None, None, row[15], row[16], None, None, None, None, row[17], row[18], None, None, None, None, row[19], row[20]])

            # Update the Google Sheet in one batch
            sheet.update(cell_range, update_data)

        print(f"Updated stocks from {i + 1} to {i + len(batch_tickers)}.")

        # Wait for a bit before processing the next batch
        time.sleep(60)  # Wait for 50 seconds between batches

    print("All stock prices updated.")

# Remove the previous scheduler setup
scheduler = BackgroundScheduler()

# This variable indicates whether the update process is currently running
is_updating = False

def update_stock_prices_and_schedule():
    global is_updating
    if not is_updating:  # Check if we are not already updating
        is_updating = True
        try:
            update_stock_prices()  # Call the function to update stock prices
        finally:
            is_updating = False  # Reset the flag after completion

    # After all stocks are updated, start over
    update_stock_prices_and_schedule()  # Call again to start the next update

# Start the first job immediately
update_stock_prices_and_schedule()


# Flask route to manually trigger stock price updates (if needed)
@app.route('/update_stock_prices')
def trigger_update_stock_prices():
    update_stock_prices()
    return jsonify({"message": "Stock prices updated manually."})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8080)
