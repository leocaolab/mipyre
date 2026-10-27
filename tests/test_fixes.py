"""Test: verify all bug fixes — middleware on GIL routes, body limit, etc."""
from mipyre import Mipyre, Response, ServerFlags

app = Mipyre()


@app.after_request
def add_header(req, resp):
    return Response(
        body=resp.body,
        status_code=resp.status_code,
        content_type=resp.content_type,
        headers={**resp.headers, "x-middleware": "yes"},
    )


@app.get("/")
def fast(req):
    return "fast"


@app.get("/numpy", runtime="main")
def numpy_route(req):
    import numpy as np
    return {"mean": float(np.mean([1, 2, 3]))}


@app.post("/echo")
def echo(req):
    return req.text()


if __name__ == "__main__":
    app.run(ServerFlags(host="127.0.0.1", port=0))
