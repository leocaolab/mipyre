"""App for tests/test_lazy_asyncio.py: only `def` routes, and asyncio is imported only
inside a handler, so its workers make no event loop; `asyncio.get_event_loop()` there is
not supported and fails with mipyre's own error. Run as a script (port 0): the test
reads the server's log."""

from mipyre import Mipyre

app = Mipyre()


@app.get("/late-loop")
def late_loop(req):
    import asyncio

    return type(asyncio.get_event_loop()).__name__


if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0, workers=1)
