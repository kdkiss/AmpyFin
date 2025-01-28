from config import MONGO_DB_USER, MONGO_DB_PASS, API_KEY, API_SECRET, BASE_URL, mongo_url
import threading
from concurrent.futures import ThreadPoolExecutor
from zoneinfo import ZoneInfo
from pymongo import MongoClient
import time
import re
from datetime import datetime, timedelta
import requests
import logging
from collections import Counter
from helper_files.alpaca_client_helper import strategies, get_latest_price, get_alpaca_crypto_tickers, dynamic_period_selector

from urllib.request import urlopen
import alpaca
from alpaca.trading.client import TradingClient
from alpaca.data.timeframe import TimeFrame, TimeFrameUnit
from alpaca.data.historical.stock import StockHistoricalDataClient
from alpaca.trading.stream import TradingStream
from alpaca.data.live.stock import StockDataStream
from alpaca.data.requests import (
    StockBarsRequest,
    StockTradesRequest,
    StockQuotesRequest
)
from alpaca.trading.requests import (
    GetAssetsRequest, 
    MarketOrderRequest, 
    LimitOrderRequest, 
    StopOrderRequest, 
    StopLimitOrderRequest, 
    TakeProfitRequest, 
    StopLossRequest, 
    TrailingStopOrderRequest, 
    GetOrdersRequest, 
    ClosePositionRequest
)
from alpaca.trading.enums import ( 
    AssetStatus, 
    AssetExchange, 
    OrderSide, 
    OrderType, 
    TimeInForce, 
    OrderClass, 
    QueryOrderStatus
)
from alpaca.common.exceptions import APIError
from strategies.talib_indicators import *
import math
import yfinance as yf
import logging
from collections import Counter

import heapq 
import certifi
ca = certifi.where()
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('rank_system.log'),  # Log messages to a file
        logging.StreamHandler()             # Log messages to the console
    ]
)


def process_ticker(ticker, mongo_client):
    try:
        current_price = None
        while current_price is None:
            try:
                current_price = get_latest_price(ticker, API_KEY, API_SECRET)
            except Exception as fetch_error:
                logging.warning(f"Error fetching price for {ticker}. Retrying... {fetch_error}")
                time.sleep(10)

        indicator_tb = mongo_client.IndicatorsDatabase
        indicator_collection = indicator_tb.Indicators
        for strategy in strategies:
            historical_data = None
            while historical_data is None:
                try:
                    period = indicator_collection.find_one({'indicator': strategy.__name__})
                    historical_data = get_data(ticker, mongo_client, period['ideal_period'], API_KEY, API_SECRET)
                except Exception as fetch_error:
                    logging.warning(f"Error fetching historical data for {ticker}. Retrying... {fetch_error}")
                    time.sleep(60)
            db = mongo_client.trading_simulator  
            holdings_collection = db.algorithm_holdings
            print(f"Processing {strategy.__name__} for {ticker}")
            strategy_doc = holdings_collection.find_one({"strategy": strategy.__name__})
            if not strategy_doc:
                logging.warning(f"Strategy {strategy.__name__} not found in database. Skipping.")
                continue

            account_cash = strategy_doc["amount_cash"]
            total_portfolio_value = strategy_doc["portfolio_value"]
            portfolio_qty = strategy_doc["holdings"].get(ticker, {}).get("quantity", 0)

            # Updated function call with mongo_client parameter
            simulate_trade(ticker, strategy, historical_data, current_price,
                           account_cash, portfolio_qty, total_portfolio_value, mongo_client)
            
        print(f"{ticker} processing completed.")
    except Exception as e:
        logging.error(f"Error in thread for {ticker}: {e}")

def simulate_trade(ticker, strategy, historical_data, current_price, account_cash, portfolio_qty, total_portfolio_value, mongo_client):
    print(f"Simulating trade for {ticker} with strategy {strategy.__name__} and quantity of {portfolio_qty}")
    action, quantity = simulate_strategy(strategy, ticker, current_price, historical_data, account_cash, portfolio_qty, total_portfolio_value)
    
    db = mongo_client.trading_simulator
    holdings_collection = db.algorithm_holdings
    points_collection = db.points_tally

    strategy_doc = holdings_collection.find_one({"strategy": strategy.__name__})
    holdings_doc = strategy_doc.get("holdings", {})
    time_delta = db.time_delta.find_one({})['time_delta']
    
    if action in ["buy"] and strategy_doc["amount_cash"] - quantity * current_price > 15000 and quantity > 0 and ((portfolio_qty + quantity) * current_price) / total_portfolio_value < 0.10:
        logging.info(f"Action: {action} | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}")
        if ticker in holdings_doc:
            current_qty = holdings_doc[ticker]["quantity"]
            new_qty = current_qty + quantity
            average_price = (holdings_doc[ticker]["price"] * current_qty + current_price * quantity) / new_qty
        else:
            new_qty = quantity
            average_price = current_price

        holdings_doc[ticker] = {
            "quantity": new_qty,
            "price": average_price
        }

        holdings_collection.update_one(
            {"strategy": strategy.__name__},
            {
                "$set": {
                    "holdings": holdings_doc,
                    "amount_cash": strategy_doc["amount_cash"] - quantity * current_price,
                    "last_updated": datetime.now()
                },
                "$inc": {"total_trades": 1}
            },
            upsert=True
        )
    
    elif action in ["sell"] and str(ticker) in holdings_doc and holdings_doc[str(ticker)]["quantity"] > 0:
        logging.info(f"Action: {action} | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}")
        current_qty = holdings_doc[ticker]["quantity"]
        sell_qty = min(quantity, current_qty)
        holdings_doc[ticker]["quantity"] = current_qty - sell_qty
        
        price_change_ratio = current_price / holdings_doc[ticker]["price"] if ticker in holdings_doc else 1

        if current_price > holdings_doc[ticker]["price"]:
            holdings_collection.update_one(
                {"strategy": strategy.__name__},
                {"$inc": {"successful_trades": 1}},
                upsert=True
            )

            if price_change_ratio < 1.05:
                points = time_delta * 1
            elif price_change_ratio < 1.1:
                points = time_delta * 1.5
            else:
                points = time_delta * 2
        else:
            if holdings_doc[ticker]["price"] == current_price:
                holdings_collection.update_one(
                    {"strategy": strategy.__name__},
                    {"$inc": {"neutral_trades": 1}}
                )
            else:
                holdings_collection.update_one(
                    {"strategy": strategy.__name__},
                    {"$inc": {"failed_trades": 1}},
                    upsert=True
                )

            if price_change_ratio > 0.975:
                points = -time_delta * 1
            elif price_change_ratio > 0.95:
                points = -time_delta * 1.5
            else:
                points = -time_delta * 2

        points_collection.update_one(
            {"strategy": strategy.__name__},
            {
                "$set" : {"last_updated": datetime.now()},
                "$inc": {"total_points": points}
            },
            upsert=True
        )
        if holdings_doc[ticker]["quantity"] == 0:
            del holdings_doc[ticker]
        holdings_collection.update_one(
            {"strategy": strategy.__name__},
            {
                "$set": {
                    "holdings": holdings_doc,
                    "amount_cash": strategy_doc["amount_cash"] + sell_qty * current_price,
                    "last_updated": datetime.now()
                },
                "$inc": {"total_trades": 1}
            },
            upsert=True
        )
    else:
        logging.info(f"Action: {action} | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}")
    print(f"Action: {action} | Ticker: {ticker} | Quantity: {quantity} | Price: {current_price}")

def update_portfolio_values(client):
    db = client.trading_simulator  
    holdings_collection = db.algorithm_holdings
    for strategy_doc in holdings_collection.find({}):
        portfolio_value = strategy_doc["amount_cash"]
        for ticker, holding in strategy_doc["holdings"].items():
            if not re.match(r'^[A-Z]+/[A-Z]+$', ticker):
                logging.warning(f"Skipping invalid ticker format: {ticker}")
                continue

            current_price = None
            while current_price is None:
                try:
                    current_price = get_latest_price(ticker, API_KEY, API_SECRET)
                except Exception as e:
                    print(f"Error fetching price for {ticker}. Retrying... {e}")
                    time.sleep(120)
            print(f"Current price of {ticker}: {current_price}")
            holding_value = holding["quantity"] * current_price
            portfolio_value += holding_value
        holdings_collection.update_one({"strategy": strategy_doc["strategy"]}, {"$set": {"portfolio_value": portfolio_value}}, upsert=True)

def update_ranks(client):
    db = client.trading_simulator
    points_collection = db.points_tally
    rank_collection = db.rank
    algo_holdings = db.algorithm_holdings
    rank_collection.delete_many({})
    q = []
    for strategy_doc in algo_holdings.find({}):
        strategy_name = strategy_doc["strategy"]
        if strategy_name == "test" or strategy_name == "test_strategy":
            continue
        total_points = points_collection.find_one({"strategy": strategy_name})["total_points"]
        if total_points > 0:
            heapq.heappush(q, (total_points * 2 + strategy_doc["portfolio_value"], strategy_doc["successful_trades"] - strategy_doc["failed_trades"], strategy_doc["amount_cash"], strategy_doc["strategy"]))
        else:
            heapq.heappush(q, (strategy_doc["portfolio_value"], strategy_doc["successful_trades"] - strategy_doc["failed_trades"], strategy_doc["amount_cash"], strategy_doc["strategy"]))
    rank = 1
    while q:
        _, _, _, strategy_name = heapq.heappop(q)
        rank_collection.insert_one({"strategy": strategy_name, "rank": rank})
        rank += 1
    db.HistoricalDatabase.delete_many({})
    print("Successfully updated ranks")
    print("Successfully deleted historical database")

def main():
    # Initialize MongoDB client
    mongo_client = MongoClient(mongo_url, tlsCAFile=ca)
    
    # Retrieve crypto tickers
    crypto_tickers = get_alpaca_crypto_tickers(API_KEY, API_SECRET, mongo_client)
    logging.info("Processing strategies for crypto tickers.")

    while True:
        threads = []
        for ticker in crypto_tickers:
            thread = threading.Thread(target=process_ticker, args=(ticker, mongo_client))
            threads.append(thread)
            thread.start()

        for thread in threads:
            thread.join()

        logging.info("Finished processing all strategies. Waiting for 120 seconds.")
        time.sleep(120)

        update_portfolio_values(mongo_client)
        update_ranks(mongo_client)

if __name__ == "__main__":
    main()