"""WebSocket echo server with a browser page.

Run:
  python examples/websocket_demo.py

Open http://127.0.0.1:8000/ in a browser, or talk to the socket from Python:
  python -c "
  from websockets.sync.client import connect
  with connect('ws://127.0.0.1:8000/ws') as ws:
      ws.send('hello')
      print(ws.recv())
  "
"""

from mipyre import Mipyre, Response

app = Mipyre()

PAGE = """<!DOCTYPE html>
<html><body>
<h1>Mipyre WebSocket Demo</h1>
<input id="msg" type="text" placeholder="Type a message...">
<button onclick="send()">Send</button>
<pre id="log"></pre>
<script>
const ws = new WebSocket(`ws://${location.host}/ws`);
const log = document.getElementById('log');
ws.onmessage = (e) => { log.textContent += e.data + '\\n'; };
ws.onopen = () => { log.textContent += '[connected]\\n'; };
ws.onclose = () => { log.textContent += '[disconnected]\\n'; };
function send() {
    const msg = document.getElementById('msg');
    ws.send(msg.value);
    msg.value = '';
}
</script>
</body></html>"""


@app.get("/")
def index(req):
    return Response(PAGE, content_type="text/html; charset=utf-8")


@app.websocket("/ws")
def echo(ws):
    try:
        while (msg := ws.recv_message()) is not None:
            if isinstance(msg, bytes):
                ws.send_bytes(msg)
            else:
                ws.send(f"echo: {msg}")
    except (ConnectionError, BlockingIOError):
        ws.close()  # the client left, or stopped reading what we send


if __name__ == "__main__":
    app.run()
