# helper_files/crypto_utils.py

import ccxt
import logging

def connect_to_ccxt(exchange_id, api_key, secret):
    """
    Connect to a cryptocurrency exchange using ccxt.

    :param exchange_id: The id of the exchange (e.g., 'binance')
    :param api_key: API key for the exchange
    :param secret: API secret for the exchange
    :return: ccxt exchange client instance
    """
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class({
        'apiKey': api_key,
        'secret': secret,
    })

    exchange.set_sandbox_mode(True) # activates testnet mode

    return exchange

def get_latest_crypto_price(ccxt_client, ticker):
    """
    Fetch the latest price for a given cryptocurrency ticker using ccxt.

    :param ccxt_client: An instance of the ccxt exchange client
    :param ticker: The cryptocurrency ticker symbol (e.g., 'BTC/USDT')
    :return: The latest price of the cryptocurrency
    """
    try:
        ticker_info = ccxt_client.fetch_ticker(ticker)
        return ticker_info['last']
    except Exception as e:
        logging.error(f"Error fetching latest price for {ticker}: {e}")
        return None

def get_crypto_tickers(exchange_id, api_key=None, secret=None):
    """
    Fetch the list of available cryptocurrency tickers from a specified exchange.

    :param exchange_id: The id of the exchange (e.g., 'binance')
    :param api_key: API key for the exchange (optional)
    :param secret: API secret for the exchange (optional)
    :return: List of available cryptocurrency tickers
    """
    exchange_class = getattr(ccxt, exchange_id)
    exchange = exchange_class({
        'apiKey': api_key,
        'secret': secret,
    })

    exchange.set_sandbox_mode(True) # activates testnet mode

    tickers = exchange.load_markets()
    return list(tickers.keys())