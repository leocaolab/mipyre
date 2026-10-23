"""A streaming upload route that says when its handler has the body's first chunk — for
tests/test_upload_streaming.py, which sends the rest of the body only after that."""

import threading

from mipyre import BodyStream, Mipyre

app = Mipyre()

# Set by the handler (on the main interpreter, in the test's process) once it has the first
# chunk; the test waits for it before sending the rest of the body.
first_chunk_seen = threading.Event()


@app.get("/")
def root(req):
    return "ok"


@app.post("/first-then-rest", runtime="main", body=BodyStream)
def first_then_rest(req):
    chunks = iter(req.stream)
    first = next(chunks)
    first_chunk_seen.set()
    rest = sum(len(chunk) for chunk in chunks)
    return {"first": len(first), "total": len(first) + rest}
