"""Test: after_request hooks work in sub-interpreter mode."""
from mipyre import Response
from mipyre.engine import MipyreApp, RuntimeChoice

app = MipyreApp()


def add_cors(req, resp):
    """after_request hook: add CORS header."""
    # Return a Response with extra headers
    return Response(
        body=resp.body,
        status_code=resp.status_code,
        content_type=resp.content_type,
        headers={**resp.headers, "x-cors": "true"},
    )


def fast_route(req):
    return "fast"


def json_route(req):
    return '{"key": "value"}'


app.after_request(add_cors)
app.route("GET", "/fast", fast_route, RuntimeChoice.Worker)
app.route("GET", "/json", json_route, RuntimeChoice.Worker)

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=0)
