"""Mipyre sub-interpreter example.

Each worker thread runs the handlers in its own Python interpreter with its own GIL:
true parallelism without free-threaded Python. Sub-interpreters are `Mipyre`'s default
runtime; `runtime="subinterp"` is spelled out here only to show where it is chosen.

Usage:
    python examples/hello_subinterp.py
"""

from mipyre import Mipyre, ServerFlags

app = Mipyre(runtime="subinterp")


@app.get("/")
def index(req):
    return "Hello from Mipyre (sub-interpreter)!"


@app.get("/hello/{name}")
def greet(req):
    return {"message": f"Hello, {req.params['name']}!"}


@app.post("/echo")
def echo(req):
    return req.text()


if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=8000))
