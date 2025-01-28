from polygon import RESTClient
from config import POLYGON_API_KEY, FINANCIAL_PREP_API_KEY, MONGO_DB_USER, MONGO_DB_PASS, API_KEY, API_SECRET, BASE_URL, mongo_url
import json
import certifi
from urllib.request import urlopen
from zoneinfo import ZoneInfo
from pymongo import MongoClient
import time
from datetime import datetime, timedelta
from helper_files.alpaca_client_helper import place_order, get_alpaca_crypto_tickers, strategies, get_latest_price, dynamic_period_selector
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import GetAssetsRequest
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from strategies.archived_strategies.trading_strategies_v1 import get_historical_data
import yfinance as yf
import logging
from collections import Counter
from statistics import median, mode
import statistics
import heapq
import requests
from strategies.talib_indicators import *


# MongoDB connection string

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('system.log'),  # Log messages to a file
        logging.StreamHandler()             # Log messages to the console
    ]
)

"""
Determines the majority decision (buy, sell, or hold) and returns the weighted median quantity for the chosen action.
"""
def weighted_majority_decision_and_median_quantity(decisions_and_quantities):  
    """  
    Determines the majority decision (buy, sell, or hold) and returns the weighted median quantity for the chosen action.  
    Groups 'strong buy' with 'buy' and 'strong sell' with 'sell'.
    Applies weights to quantities based on strategy coefficients.  
    """  
    buy_decisions = ['buy', 'strong buy']  
    sell_decisions = ['sell', 'strong sell']  

    weighted_buy_quantities = []
    weighted_sell_quantities = []
    buy_weight = 0
    sell_weight = 0
    hold_weight = 0
    
    # Process decisions with weights
    for decision, quantity, weight in decisions_and_quantities:
        if decision in buy_decisions:
            weighted_buy_quantities.extend([quantity])
            buy_weight += weight
        elif decision in sell_decisions:
            weighted_sell_quantities.extend([quantity])
            sell_weight += weight
        elif decision == 'hold':
            hold_weight += weight
    
    
    # Determine the majority decision based on the highest accumulated weight
    if buy_weight > sell_weight and buy_weight > hold_weight:
        return 'buy', median(weighted_buy_quantities) if weighted_buy_quantities else 0, buy_weight, sell_weight, hold_weight
    elif sell_weight > buy_weight and sell_weight > hold_weight:
        return 'sell', median(weighted_sell_quantities) if weighted_sell_quantities else 0, buy_weight, sell_weight, hold_weight
    else:
        return 'hold', 0, buy_weight, sell_weight, hold_weight

from pymongo.errors import PyMongoError, ServerSelectionTimeoutError

def main():
    """
    Main function to control the workflow based on the market's status.
    """
    crypto_tickers = []
    trading_client = TradingClient(API_KEY, API_SECRET)
    mongo_client = MongoClient(mongo_url, tlsCAFile=certifi.where(), serverSelectionTimeoutMS=5000)  # Set a 5-second timeout
    db = mongo_client.trades
    asset_collection = db.assets_quantities
    limits_collection = db.assets_limit
    sim_db = mongo_client.trading_simulator
    rank_collection = sim_db.rank
    r_t_c_collection = sim_db.rank_to_coefficient
    indicator_tb = mongo_client.IndicatorsDatabase
    indicator_collection = indicator_tb.Indicators
    strategy_to_coefficient = {}

    # Retrieve crypto tickers
    crypto_tickers = get_alpaca_crypto_tickers(API_KEY, API_SECRET, mongo_client)
    logging.info("Processing strategies for crypto tickers.")

    while True:
        account = trading_client.get_account()
        buy_heap = []
        suggestion_heap = []

        for ticker in crypto_tickers:
            decisions_and_quantities = []
            try:
                trading_client = TradingClient(API_KEY, API_SECRET)
                account = trading_client.get_account()
                buying_power = float(account.cash)
                portfolio_value = float(account.portfolio_value)
                cash_to_portfolio_ratio = buying_power / portfolio_value
                trades_db = mongo_client.trades
                portfolio_collection = trades_db.portfolio_values
                
                try:
                    portfolio_collection.update_one({"name": "portfolio_percentage"}, {"$set": {"portfolio_value": (portfolio_value - 50491.13) / 50491.13}})
                except ServerSelectionTimeoutError:
                    logging.error("MongoDB server selection timeout. Check your connection.")
                except PyMongoError as e:
                    logging.error(f"Error updating portfolio_percentage: {e}")
                
                current_price = None
                while current_price is None:
                    try:
                        current_price = get_latest_price(ticker, API_KEY, API_SECRET)
                    except Exception as e:
                        print(f"Error fetching price for {ticker}: {e}. Retrying...")
                        time.sleep(10)
                print(f"Current price of {ticker}: {current_price}")

                asset_info = asset_collection.find_one({'symbol': ticker})
                portfolio_qty = asset_info['quantity'] if asset_info else 0.0
                print(f"Portfolio quantity for {ticker}: {portfolio_qty}")
                
                limit_info = limits_collection.find_one({'symbol': ticker})
                if limit_info:
                    stop_loss_price = limit_info['stop_loss_price']
                    take_profit_price = limit_info['take_profit_price']
                    if current_price <= stop_loss_price or current_price >= take_profit_price:
                        print(f"Executing SELL order for {ticker} due to stop-loss or take-profit condition")
                        quantity = portfolio_qty
                        order = place_order(trading_client, symbol=ticker, side=OrderSide.SELL, quantity=quantity, mongo_client=mongo_client)
                        logging.info(f"Executed SELL order for {ticker}: {order}")
                        continue
                                    
                for strategy in strategies:
                    try:
                        logging.info(f"Executing strategy {strategy.__name__} for ticker {ticker}")
                        historical_data = None
                        while historical_data is None:
                            try:
                                period = indicator_collection.find_one({'indicator': strategy.__name__})
                                historical_data = get_data(ticker, mongo_client, period['ideal_period'], API_KEY, API_SECRET)
                            except Exception as e:
                                logging.error(f"Error fetching historical data for {ticker}: {e}. Retrying...")
                                time.sleep(10)
                        
                        if historical_data is None or 'Close' not in historical_data.columns:
                            logging.warning(f"Invalid or incomplete historical data for {ticker}. Skipping strategy {strategy.__name__}.")
                            continue

                        decision, quantity = simulate_strategy(strategy, ticker, current_price, historical_data, buying_power, portfolio_qty, portfolio_value)
                        logging.info(f"Strategy {strategy.__name__} result for {ticker}: Decision={decision}, Quantity={quantity}")
                    except Exception as e:
                        logging.error(f"Error executing strategy {strategy.__name__} for {ticker}: {e}")

                
                print(f"Ticker: {ticker}, Decision: {decision}, Quantity: {quantity}, Weights: Buy: {buy_weight}, Sell: {sell_weight}, Hold: {hold_weight}")
                print(f"Cash: {account.cash}")

                if decision == "buy" and float(account.cash) > 15000 and (((quantity + portfolio_qty) * current_price) / portfolio_value) < 0.1:
                    heapq.heappush(buy_heap, (-(buy_weight - (sell_weight + (hold_weight * 0.5))), quantity, ticker))
                elif decision == "sell" and portfolio_qty > 0:
                    print(f"Executing SELL order for {ticker}")
                    print(f"Executing quantity of {quantity} for {ticker}")
                    quantity = max(quantity, 1)
                    order = place_order(trading_client, symbol=ticker, side=OrderSide.SELL, quantity=quantity, mongo_client=mongo_client)
                    logging.info(f"Executed SELL order for {ticker}: {order}")
                elif portfolio_qty == 0.0 and buy_weight > sell_weight and (((quantity + portfolio_qty) * current_price) / portfolio_value) < 0.1 and float(account.cash) > 15000:
                    max_investment = portfolio_value * 0.10
                    buy_quantity = min(int(max_investment // current_price), int(buying_power // current_price))
                    if buy_weight > 2050000:
                        buy_quantity = max(buy_quantity, 2)
                        buy_quantity = buy_quantity // 2
                        print(f"Suggestions for buying for {ticker} with a weight of {buy_weight} and quantity of {buy_quantity}")
                        heapq.heappush(suggestion_heap, (-(buy_weight - sell_weight), buy_quantity, ticker))
                    else:
                        logging.info(f"Holding for {ticker}, no action taken.")
                else:
                    logging.info(f"Holding for {ticker}, no action taken.")
                
            except Exception as e:
                logging.error(f"Error processing {ticker}: {e}")
        
        trading_client = TradingClient(API_KEY, API_SECRET)
        account = trading_client.get_account()
        while (buy_heap or suggestion_heap) and float(account.cash) > 15000:
            try:
                trading_client = TradingClient(API_KEY, API_SECRET)
                account = trading_client.get_account()
                print(f"Cash: {account.cash}")
                if buy_heap and float(account.cash) > 15000:
                    _, quantity, ticker = heapq.heappop(buy_heap)
                    print(f"Executing BUY order for {ticker} of quantity {quantity}")
                    order = place_order(trading_client, symbol=ticker, side=OrderSide.BUY, quantity=quantity, mongo_client=mongo_client)
                    logging.info(f"Executed BUY order for {ticker}: {order}")
                elif suggestion_heap and float(account.cash) > 15000:
                    _, quantity, ticker = heapq.heappop(suggestion_heap)
                    print(f"Executing BUY order for {ticker} of quantity {quantity}")
                    order = place_order(trading_client, symbol=ticker, side=OrderSide.BUY, quantity=quantity, mongo_client=mongo_client)
                    logging.info(f"Executed BUY order for {ticker}: {order}")
                time.sleep(5)
                """
                This is here so order will propagate through and we will have an accurate cash balance recorded
                """
            except Exception as e:
                print(f"Error occurred while executing buy order: {e}. Continuing...")
                break
        
        print("Sleeping for 60 seconds...")
        time.sleep(60)

if __name__ == "__main__":
    main()
