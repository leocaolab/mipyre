"""The order route. pydantic validates its body and can't load in a sub-interpreter, so it
runs on the main interpreter and only the main interpreter imports this module."""

import random
import time

from pydantic import BaseModel, Field

from mipyre import Router

from trading.market import get_live_price

router = Router(runtime="main")


class OrderRequest(BaseModel):
    ticker: str = Field(max_length=5)
    side: str = Field(pattern="^(buy|sell)$")
    quantity: int = Field(gt=0, le=100000)
    price: float = Field(gt=0)


@router.post("/order", model=OrderRequest)
def submit_order(req, order: OrderRequest):
    """Submit a trade order. Pydantic validates ticker, side, quantity, price."""
    # Get current price
    current = get_live_price(order.ticker)

    # Simple fill simulation
    slippage = random.uniform(0, 0.001)
    fill_price = order.price * ((1 + slippage) if order.side == "buy" else (1 - slippage))

    return {
        "status": "filled",
        "order_id": f"ORD-{random.randint(100000, 999999)}",
        "ticker": order.ticker,
        "side": order.side,
        "quantity": order.quantity,
        "requested_price": order.price,
        "fill_price": round(fill_price, 2),
        "market_price": current["price"],
        "slippage_bps": round(slippage * 10000, 1),
        "timestamp": time.time(),
    }
