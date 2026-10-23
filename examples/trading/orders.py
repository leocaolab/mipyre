"""The order route. pydantic validates its body and can't load in a sub-interpreter, and the
route records each order in app.state, which a worker process can't reach; it is one
low-traffic write, so it runs on runtime="main" and only the main interpreter imports this
module."""

import json
import random
import time
from typing import Literal

from pydantic import BaseModel, Field, field_validator

from mipyre import Response, Router, SharedState

from trading.market import BASE_PRICES, get_live_price

router = Router(runtime="main")

MAX_SLIPPAGE = 0.001


class OrderRequest(BaseModel):
    ticker: str
    side: Literal["buy", "sell"]
    quantity: int = Field(gt=0, le=100_000)
    price: float = Field(gt=0)

    @field_validator("ticker")
    @classmethod
    def known_ticker(cls, ticker: str) -> str:
        if ticker not in BASE_PRICES:
            raise ValueError(f"unknown ticker {ticker!r}")
        return ticker


def _state() -> SharedState:
    # trading.main imports this module while it builds the app, so the app is looked up
    # when a request comes, not at import.
    from trading.main import app

    return app.state


@router.post("/order", body=OrderRequest)
def submit_order(order: OrderRequest):
    state = _state()
    slippage = random.uniform(0, MAX_SLIPPAGE)
    fill_price = order.price * (1 + slippage if order.side == "buy" else 1 - slippage)
    filled = {
        "order_id": state.incr("orders:last_id", 1),
        "status": "filled",
        "ticker": order.ticker,
        "side": order.side,
        "quantity": order.quantity,
        "requested_price": order.price,
        "fill_price": round(fill_price, 2),
        "market_price": get_live_price(order.ticker)["price"],
        "slippage_bps": round(slippage * 10_000, 1),
        "timestamp": time.time(),
    }
    state[f"order:{filled['order_id']}"] = json.dumps(filled)
    return Response(filled, status_code=201)
