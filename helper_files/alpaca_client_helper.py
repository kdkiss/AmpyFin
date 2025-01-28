from pymongo import MongoClient
from alpaca.trading.client import TradingClient
from alpaca.trading.requests import MarketOrderRequest
from alpaca.trading.enums import OrderSide, TimeInForce
from datetime import datetime, timedelta
import logging
import requests
import numpy as np
import sys
from pathlib import Path

parent_dir = Path(__file__).resolve().parent.parent
sys.path.append(str(parent_dir))
from strategies.talib_indicators import (get_data, BBANDS_indicator, DEMA_indicator, EMA_indicator, HT_TRENDLINE_indicator, KAMA_indicator, MA_indicator, MAMA_indicator, MAVP_indicator, MIDPOINT_indicator,
                                         MIDPRICE_indicator, SAR_indicator, ADX_indicator, ADXR_indicator, APO_indicator, AROON_indicator, AROONOSC_indicator, BOP_indicator, CCI_indicator, CMO_indicator,
                                         DX_indicator, MACD_indicator, MACDEXT_indicator, AD_indicator, ADOSC_indicator, OBV_indicator, HT_DCPERIOD_indicator, HT_DCPHASE_indicator, HT_PHASOR_indicator,
                                         HT_SINE_indicator, HT_TRENDMODE_indicator, AVGPRICE_indicator, MEDPRICE_indicator, TYPPRICE_indicator, WCLPRICE_indicator, ATR_indicator, NATR_indicator,
                                         TRANGE_indicator, CDL2CROWS_indicator, CDL3BLACKCROWS_indicator, CDL3INSIDE_indicator, CDL3LINESTRIKE_indicator, CDL3OUTSIDE_indicator, CDL3STARSINSOUTH_indicator,
                                         CDL3WHITESOLDIERS_indicator, BETA_indicator, CORREL_indicator, LINEARREG_indicator, LINEARREG_ANGLE_indicator, LINEARREG_INTERCEPT_indicator, LINEARREG_SLOPE_indicator,
                                         STDDEV_indicator, TSF_indicator, VAR_indicator)

overlap_studies = [BBANDS_indicator, DEMA_indicator, EMA_indicator, HT_TRENDLINE_indicator, KAMA_indicator, MA_indicator, MAMA_indicator, MAVP_indicator, MIDPOINT_indicator, MIDPRICE_indicator, SAR_indicator]
momentum_indicators = [ADX_indicator, ADXR_indicator, APO_indicator, AROON_indicator, AROONOSC_indicator, BOP_indicator, CCI_indicator, CMO_indicator, DX_indicator, MACD_indicator, MACDEXT_indicator]
volume_indicators = [AD_indicator, ADOSC_indicator, OBV_indicator]
cycle_indicators = [HT_DCPERIOD_indicator, HT_DCPHASE_indicator, HT_PHASOR_indicator, HT_SINE_indicator, HT_TRENDMODE_indicator]
price_transforms = [AVGPRICE_indicator, MEDPRICE_indicator, TYPPRICE_indicator, WCLPRICE_indicator]
volatility_indicators = [ATR_indicator, NATR_indicator, TRANGE_indicator]
pattern_recognition = [CDL2CROWS_indicator, CDL3BLACKCROWS_indicator, CDL3INSIDE_indicator, CDL3LINESTRIKE_indicator, CDL3OUTSIDE_indicator, CDL3STARSINSOUTH_indicator, CDL3WHITESOLDIERS_indicator]
statistical_functions = [BETA_indicator, CORREL_indicator, LINEARREG_indicator, LINEARREG_ANGLE_indicator, LINEARREG_INTERCEPT_indicator, LINEARREG_SLOPE_indicator, STDDEV_indicator, TSF_indicator, VAR_indicator]

strategies = overlap_studies + momentum_indicators + volume_indicators + cycle_indicators + price_transforms + volatility_indicators + pattern_recognition + statistical_functions

# MongoDB connection helper
def connect_to_mongo(mongo_url):
    """Connect to MongoDB and return the client."""
    return MongoClient(mongo_url)

# Helper to place an order
def place_order(trading_client, symbol, side, quantity, mongo_client):
    """
    Place a market order and log the order to MongoDB.

    :param trading_client: The Alpaca trading client instance
    :param symbol: The crypto symbol to trade
    :param side: Order side (OrderSide.BUY or OrderSide.SELL)
    :param qty: Quantity to trade
    :param mongo_client: MongoDB client instance
    :return: Order result from Alpaca API
    """
    
    market_order_data = MarketOrderRequest(
        symbol=symbol,
        qty=quantity,
        side=side,
        time_in_force=TimeInForce.DAY
    )
    order = trading_client.submit_order(market_order_data)
    qty = round(quantity, 3)
    current_price = get_latest_price(symbol)
    stop_loss_price = round(current_price * 0.97, 2)  # 3% loss
    take_profit_price = round(current_price * 1.05, 2)  # 5% profit

    # Log trade details to MongoDB
    db = mongo_client.trades
    db.paper.insert_one({
        'symbol': symbol,
        'qty': qty,
        'side': side.name,
        'time_in_force': TimeInForce.DAY.name,
        'time': datetime.now()
    })

    # Track assets as well
    assets = db.assets_quantities
    limits = db.assets_limit

    if side == OrderSide.BUY:
        assets.update_one({'symbol': symbol}, {'$inc': {'quantity': qty}}, upsert=True)
        limits.update_one(
            {'symbol': symbol},
            {'$set': {'stop_loss_price': stop_loss_price, 'take_profit_price': take_profit_price}},
            upsert=True
        )
    elif side == OrderSide.SELL:
        assets.update_one({'symbol': symbol}, {'$inc': {'quantity': -qty}}, upsert=True)
        if assets.find_one({'symbol': symbol})['quantity'] == 0:
            assets.delete_one({'symbol': symbol})
            limits.delete_one({'symbol': symbol})

    return order

# Helper to retrieve Alpaca crypto tickers
def get_alpaca_crypto_tickers(api_key, api_secret, mongo_client):
    """
    Connects to Alpaca API and retrieves crypto tickers.

    :param api_key: The Alpaca API key
    :param api_secret: The Alpaca API secret
    :param mongo_client: MongoDB client instance
    :return: List of Alpaca crypto ticker symbols.
    """
    url = "https://api.alpaca.markets/v2/assets?asset_class=crypto"
    headers = {
        "APCA-API-KEY-ID": api_key,
        "APCA-API-SECRET-KEY": api_secret
    }
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        assets = response.json()
        crypto_tickers = [asset["symbol"] for asset in assets if asset["status"] == "active"]
        
        # Store the tickers in MongoDB
        db = mongo_client.stock_list
        crypto_tickers_collection = db.crypto_tickers
        crypto_tickers_collection.delete_many({})  # Clear existing data
        crypto_tickers_collection.insert_many([{"symbol": ticker} for ticker in crypto_tickers])  # Insert new data
        
        return crypto_tickers
    else:
        raise Exception(f"Error fetching Alpaca crypto tickers: {response.status_code} - {response.text}")

# Market status checker helper
def market_status(polygon_client):
    """
    Check market status using the Polygon API.

    :param polygon_client: An instance of the Polygon RESTClient
    :return: Current market status ('open', 'early_hours', 'closed')
    """
    try:
        status = polygon_client.get_market_status()
        if status.exchanges.nasdaq == "open" and status.exchanges.nyse == "open":
            return "open"
        elif status.early_hours:
            return "early_hours"
        else:
            return "closed"
    except Exception as e:
        logging.error(f"Error retrieving market status: {e}")
        return "error"

# Helper to get latest price
def get_latest_price(ticker):  
   """  
   Fetch the latest price for a given crypto ticker using Alpaca API.  
  
   :param ticker: The crypto ticker symbol  
   :return: The latest price of the crypto  
   """  
   try:  
      url = f"https://data.alpaca.markets/v1beta3/crypto/us/latest/quotes?symbols={ticker}"
      response = requests.get(url, headers={
          "APCA-API-KEY-ID": api_key,
          "APCA-API-SECRET-KEY": api_secret
      })
      if response.status_code == 200:
          data = response.json()
          return round(data['quotes'][ticker]['ap'], 2)  # 'ap' is the ask price
      else:
          logging.error(f"Error fetching latest price for {ticker}: {response.status_code} - {response.text}")  
          return None
   except Exception as e:  
      logging.error(f"Error fetching latest price for {ticker}: {e}")  
      return None

def dynamic_period_selector(api_key, api_secret, ticker):
    """
    Determines the best period to use for fetching historical data.
    
    Args:
    - ticker (str): Crypto ticker symbol.
    
    Returns:
    - str: Optimal period for historical data retrieval.
    """
    periods = ['1Min', '5Min', '15Min', '1H', '1D']
    volatility_scores = []

    for period in periods:
        try:
            end_time = datetime.now()
            start_time = end_time - timedelta(days=365)
            url = f"https://data.alpaca.markets/v1beta3/crypto/us/bars?symbols={ticker}&timeframe={period}&start={start_time.isoformat()}&end={end_time.isoformat()}"
            response = requests.get(url, headers={
                "APCA-API-KEY-ID": api_key,
                "APCA-API-SECRET-KEY": api_secret
            })
            if response.status_code == 200:
                data = response.json()
                bars = data['bars'][ticker]
                if not bars:
                    continue
                
                # Calculate metrics for decision-making
                close_prices = [bar['c'] for bar in bars]  # 'c' is the close price
                volatility = np.std(close_prices)
                trend_strength = abs(close_prices[-1] - close_prices[0]) / close_prices[0]
                
                # Combine metrics into a single score (weight them as desired)
                score = volatility * 0.7 + trend_strength * 0.3
                volatility_scores.append((period, score))
            else:
                logging.error(f"Error fetching data for period {period}: {response.status_code} - {response.text}")
                continue
        except Exception as e:
            logging.error(f"Error fetching data for period {period}: {e}")
            continue

    # Select the period with the highest score
    optimal_period = min(volatility_scores, key=lambda x: x[1])[0] if volatility_scores else '1D'
    return optimal_period