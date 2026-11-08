"""WebSocket binary test server. Binds a port the kernel picks and prints it on the
"Listening on" startup line; pass that port to ws_binary_client.py."""
from mipyre import Mipyre, ServerFlags

app = Mipyre()

@app.websocket("/echo")
def echo(ws, msg):
    return msg if isinstance(msg, bytes) else f"echo: {msg}"

@app.get("/")
def index(req):
    return "ok"

if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
