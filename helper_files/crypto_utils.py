import ccxt
import logging
import pandas as pd

def connect_to_ccxt(exchange_id, api_key, secret):
    """Connect to a cryptocurrency exchange using ccxt."""
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class({
        'apiKey': api_key,
        'secret': secret,
    })
    exchange.set_sandbox_mode(True) # activates testnet mode
    return exchange

def get_latest_crypto_price(ccxt_client, ticker):
    """Fetch the latest price for a given cryptocurrency ticker using ccxt."""
    try:
        ticker_info = ccxt_client.fetch_ticker(ticker)
        return ticker_info['last']
    except Exception as e:
        logging.error(f"Error fetching latest price for {ticker}: {e}")
        return None

def get_crypto_tickers(exchange_id, api_key=None, secret=None):
    """Fetch the list of available cryptocurrency tickers from a specified exchange."""
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class({
        'apiKey': api_key,
        'secret': secret,
    })
    exchange.set_sandbox_mode(True) # activates testnet mode
    tickers = exchange.load_markets()
    return list(tickers.keys())

def get_crypto_data(ticker, ccxt_client, period='1y'):
    """Retrieve historical data for a given cryptocurrency ticker."""
    try:
        ohlcv = ccxt_client.fetch_ohlcv(ticker, timeframe='1d', limit=365)
        data = pd.DataFrame(ohlcv, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume'])
        data['Date'] = pd.to_datetime(data['timestamp'], unit='ms')
        data.set_index('Date', inplace=True)
        data.drop(columns=['timestamp'], inplace=True)
        
        # Rename columns from lowercase to uppercase
        data.rename(columns={'open': 'Open', 'high': 'High', 'low': 'Low', 'close': 'Close', 'volume': 'Volume'}, inplace=True)
        
        return data
    except Exception as e:
        logging.error(f"Error fetching data for {ticker}: {e}")
        return pd.DataFrame()  # Return an empty DataFrame on error