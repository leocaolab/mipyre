"""Simulated market data. The base prices are constants, so every interpreter and every
worker process quotes around the same price."""

import random
import time

BASE_PRICES = {
    "AAPL": 230.0, "TSLA": 250.0, "NVDA": 120.0, "MSFT": 420.0, "AMZN": 185.0,
    "GOOG": 165.0, "META": 560.0, "JPM": 215.0, "V": 280.0, "WMT": 80.0,
}
TICKERS = list(BASE_PRICES)


def get_live_price(ticker: str) -> dict:
    """Return a quote for one of TICKERS; KeyError for any other ticker."""
    price = BASE_PRICES[ticker] * (1 + random.gauss(0, 0.002))
    return {
        "ticker": ticker,
        "price": round(price, 2),
        "bid": round(price * 0.999, 2),
        "ask": round(price * 1.001, 2),
        "volume": random.randint(10_000, 1_000_000),
        "timestamp": time.time(),
    }
