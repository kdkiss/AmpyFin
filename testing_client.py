import threading
import random
import time
from pymongo import MongoClient
from config import API_KEY, API_SECRET, mongo_url
from helper_files.alpaca_client_helper import get_alpaca_crypto_tickers, strategies, get_latest_price, dynamic_period_selector
from strategies.talib_indicators import get_data, simulate_strategy

def test_strategies(api_key, api_secret):
    # Initialize the MongoDB client
    mongo_client = MongoClient(mongo_url)
    
    # Retrieve Alpaca crypto tickers
    tickers = get_alpaca_crypto_tickers(api_key, api_secret, mongo_client)
    
    periods = ['1d', '5d', '1mo', '3mo', '6mo', '1y', '2y', '5y', 'ytd', 'max']
    account_cash = 50000
    portfolio_qty = 10
    total_portfolio_value = 500000
    
    # Define test parameters
    for ticker in tickers:
        current_price = get_latest_price(ticker, api_key, api_secret)
        historical_data = get_data(ticker, mongo_client, period='1y', api_key=api_key, api_secret=api_secret)
        for strategy in strategies:
            try:
                decision = simulate_strategy(strategy, ticker, current_price, historical_data, account_cash, portfolio_qty, total_portfolio_value)
                print(f"{strategy.__name__} : {decision} : {ticker}")
            except Exception as e:
                print(f"ERROR processing {ticker} for {strategy.__name__}: {e}")
        time.sleep(5)
    
    for ticker in tickers:
        print(f"{ticker} : {dynamic_period_selector(api_key, api_secret, ticker)}")
    
    mongo_client.close()

if __name__ == "__main__":
    print(get_latest_price('BTC/USD'))
    """
    test_strategies(API_KEY, API_SECRET)
    """