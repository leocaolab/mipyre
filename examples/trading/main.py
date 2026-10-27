"""The app and its sub-interpreter and main-interpreter routes. Every interpreter imports
this module, so it imports nothing a sub-interpreter can't load: numpy and pandas live in
trading.analytics, pydantic in trading.orders."""

import json
import random
import time

from mipyre import Mipyre, Response, rpc

from trading.market import TICKERS, get_live_price

app = Mipyre()
app.include_router("trading.orders:router")
app.include_router("trading.analytics:router")
app.enable_cors(allow_origins="*")
app.enable_access_log()

SIGNALS = ("buy", "sell", "hold")
DEFAULT_SIGNAL_TICKERS = TICKERS[:5]


@app.get("/")
def index(req):
    return {
        "service": "Mipyre Trading API",
        "tickers": TICKERS,
        "endpoints": [
            "GET /market/{ticker}: a quote",
            "GET /market: every ticker's quote",
            "POST /order: submit an order (validated with pydantic)",
            "GET /orders/{order_id}: an order that was filled",
            "GET /analytics/portfolio: portfolio analytics with numpy and pandas",
            "GET /analytics/correlation: a correlation matrix with pandas",
            "WS /ws/prices: quotes over a WebSocket",
            "POST /rpc/get_signals: trading signals over MsgPack RPC",
        ],
    }


@app.get("/market/{ticker}")
def market_quote(req, ticker):
    ticker = ticker.upper()
    if ticker not in TICKERS:
        return Response({"error": f"unknown ticker {ticker!r}"}, status_code=404)
    return get_live_price(ticker)


@app.get("/market")
def market_all(req):
    return {"quotes": {t: get_live_price(t) for t in TICKERS}, "count": len(TICKERS)}


@app.get("/orders/{order_id}")
def get_order(req, order_id):
    order = app.state.get(f"order:{order_id}")
    if order is None:
        return Response({"error": f"no order {order_id}"}, status_code=404)
    return Response(order, content_type="application/json")


@rpc(app)
def get_signals(data):
    tickers = [t for t in data.get("tickers", DEFAULT_SIGNAL_TICKERS) if t in TICKERS]
    signals = [
        {
            "ticker": t,
            "signal": random.choice(SIGNALS),
            "confidence": round(random.uniform(0.5, 0.99), 2),
            "price": get_live_price(t)["price"],
        }
        for t in tickers
    ]
    return {"signals": signals, "timestamp": time.time()}


@app.websocket("/ws/prices")
async def price_stream(ws):
    """Answer each text message, a comma-separated list of tickers or "all", with their
    quotes."""
    async for msg in ws:
        if isinstance(msg, bytes):
            continue
        wanted = TICKERS if msg.strip().lower() == "all" else [t.strip().upper() for t in msg.split(",")]
        try:
            await ws.send(json.dumps({t: get_live_price(t) for t in wanted if t in TICKERS}))
        except ConnectionError:
            return  # the client left
