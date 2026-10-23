"""Trading API: quotes on the sub-interpreters, numpy and pandas analytics in worker
processes, a pydantic-validated order on the main interpreter, a WebSocket price feed and
MsgPack RPC.

Layout (each interpreter imports only the modules it serves):
  trading/main.py       the app, its sub-interpreter routes, the WebSocket and the RPC method
  trading/orders.py     Router(runtime="main"): POST /order
  trading/analytics.py  Router(runtime="process"): /analytics/portfolio, /analytics/correlation
  trading/market.py     the simulated market data they all use

Run:
  pip install mipyre pydantic numpy pandas msgpack httpx
  cd examples && python trading_api.py      (in production: mipyre run trading_api:app)

Try:
  curl http://127.0.0.1:8000/market/AAPL
  curl -X POST http://127.0.0.1:8000/order -H 'content-type: application/json' \
    -d '{"ticker": "AAPL", "side": "buy", "quantity": 100, "price": 230.5}'
  curl http://127.0.0.1:8000/orders/1
  curl http://127.0.0.1:8000/analytics/portfolio
  python -c "
  from mipyre import RPCClient
  with RPCClient('http://127.0.0.1:8000') as c:
      print(c.get_signals(tickers=['AAPL', 'TSLA']))
  "
"""

from trading.main import app

if __name__ == "__main__":
    app.run()
