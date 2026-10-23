"""The analytics routes. numpy and pandas can't load in a sub-interpreter, and these routes
need nothing but the request, so they run on runtime="process": in worker processes, in
parallel, and only the workers import this module."""

import numpy as np
import pandas as pd

from mipyre import Router

from trading.market import get_live_price

router = Router(runtime="process")

PORTFOLIO = pd.DataFrame(
    {
        "ticker": ["AAPL", "TSLA", "NVDA", "MSFT"],
        "shares": [500, 200, 100, 300],
        "avg_cost": [145.0, 220.0, 450.0, 380.0],
    }
)
CORRELATION_DAYS = 60
DAILY_VOLATILITY = 0.02


@router.get("/analytics/portfolio")
def portfolio(req):
    positions = PORTFOLIO.assign(price=[get_live_price(t)["price"] for t in PORTFOLIO["ticker"]])
    positions["market_value"] = (positions["shares"] * positions["price"]).round(2)
    positions["pnl"] = (positions["market_value"] - positions["shares"] * positions["avg_cost"]).round(2)
    positions["pnl_pct"] = ((positions["price"] / positions["avg_cost"] - 1) * 100).round(2)
    weights = positions["market_value"] / positions["market_value"].sum()
    return {
        "positions": positions.to_dict(orient="records"),
        "summary": {
            "total_value": round(float(positions["market_value"].sum()), 2),
            "total_pnl": round(float(positions["pnl"].sum()), 2),
            "max_position_weight_pct": round(float(weights.max()) * 100, 1),
            "num_positions": len(positions),
        },
    }


@router.get("/analytics/correlation")
def correlation(req):
    tickers = list(PORTFOLIO["ticker"])
    rng = np.random.default_rng()
    returns = pd.DataFrame(rng.normal(0, DAILY_VOLATILITY, (CORRELATION_DAYS, len(tickers))), columns=tickers)
    return {
        "tickers": tickers,
        "correlation_matrix": returns.corr().round(3).values.tolist(),
        "days": CORRELATION_DAYS,
    }
