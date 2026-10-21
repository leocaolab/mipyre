"""Simulated market data. Each interpreter that imports this module draws its own base
prices."""

import random
import time

TICKERS = ["AAPL", "TSLA", "NVDA", "MSFT", "AMZN", "GOOG", "META", "JPM", "V", "WMT"]
BASE_PRICES = {t: random.uniform(50, 500) for t in TICKERS}


def get_live_price(ticker: str) -> dict:
    base = BASE_PRICES.get(ticker, 100.0)
    price = base * (1 + random.gauss(0, 0.002))
    return {
        "ticker": ticker,
        "price": round(price, 2),
        "bid": round(price * 0.999, 2),
        "ask": round(price * 1.001, 2),
        "volume": random.randint(10000, 1000000),
        "timestamp": time.time(),
    }
