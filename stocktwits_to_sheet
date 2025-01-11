import os
import json
import requests
import gspread
from google.oauth2.service_account import Credentials
import time

# Set up Google Sheets API credentials
SCOPE = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive"
]

# Load the Google credentials from the environment variable
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON")

if GOOGLE_CREDENTIALS_JSON is None:
    raise ValueError("GOOGLE_CREDENTIALS_JSON environment variable is not set.")

# Create credentials from the JSON string
creds = Credentials.from_service_account_info(json.loads(GOOGLE_CREDENTIALS_JSON), scopes=SCOPE)

# Authorize the Google Sheets client
client = gspread.authorize(creds)

# Open the Google Sheet
SPREADSHEET_NAME = 'Flux Capacitor'
sheet = client.open(SPREADSHEET_NAME).worksheet("Trending Stocks")

# Constants
STOCKTWITS_API_URL = "https://api.stocktwits.com/api/2/streams/trending.json"

def fetch_trending_stocks():
    """Fetch trending stocks from Stocktwits API."""
    response = requests.get(STOCKTWITS_API_URL)
    response.raise_for_status()  # Raise an error for bad status codes
    data = response.json()
    
    trending = []
    for message in data.get("messages", []):
        symbol_info = message.get("symbol", {})
        trending.append([
            symbol_info.get("symbol", "-"),
            symbol_info.get("title", "-"),
            message.get("created_at", "-"),
        ])
    return trending

def update_google_sheet(data):
    """Update Google Sheet with trending stocks."""
    # Add headers
    headers = [["Symbol", "Title", "Created At"]]
    data = headers + data

    # Update Google Sheet (starting from A1)
    sheet.clear()  # Clear the sheet before updating
    sheet.update("A1", data)  # Update with new data

def main():
    while True:
        try:
            # Fetch trending stocks
            print("Fetching trending stocks...")
            trending_stocks = fetch_trending_stocks()

            # Update Google Sheet
            print("Updating Google Sheet...")
            update_google_sheet(trending_stocks)

            print("Update completed. Waiting for the next update...")
            time.sleep(3600)  # Run every hour
        except Exception as e:
            print(f"An error occurred: {e}")
            time.sleep(60)  # Wait before retrying

if __name__ == "__main__":
    main()
