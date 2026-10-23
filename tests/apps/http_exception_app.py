"""App for tests/test_http_exception.py: handlers and a hook that raise `HTTPException` on
every runtime, the engine's own refusals beside them, and handlers that read a bad body."""

from mipyre import HTTPException, Mipyre
from mipyre.uploads import parse_multipart

app = Mipyre(log_config={"level": "debug"})
app.max_body_size = 1024

RUNTIMES = ("subinterp", "main", "process")


def _register(runtime):
    prefix = f"/{runtime}"

    @app.get(f"{prefix}/missing", runtime=runtime)
    def missing(req):
        raise HTTPException(404, "no order 17")

    @app.get(f"{prefix}/busy", runtime=runtime)
    def busy(req):
        raise HTTPException(503, {"retry": "later"}, headers={"Retry-After": "5"})

    @app.get(f"{prefix}/bare", runtime=runtime)
    def bare(req):
        raise HTTPException(404)

    @app.post(f"{prefix}/json", runtime=runtime)
    def json_body(req):
        return {"got": req.json()}


for _runtime in RUNTIMES:
    _register(_runtime)


@app.get("/async/missing")
async def async_missing(req):
    raise HTTPException(404, "no order 17")


@app.get("/async/busy")
async def async_busy(req):
    raise HTTPException(503, {"retry": "later"}, headers={"Retry-After": "5"})


@app.get("/async/bare")
async def async_bare(req):
    raise HTTPException(404)


@app.post("/async/json")
async def async_json(req):
    return {"got": req.json()}


@app.post("/text")
def text_body(req):
    return {"got": req.text()}


@app.post("/upload")
def upload(req):
    form = parse_multipart(req, max_parts=2)
    return {"fields": sorted(form)}


@app.get("/guarded")
def guarded(req):
    return {"ran": True}


@app.before_request
def login_required(req):
    if req.path == "/guarded":
        raise HTTPException(401, "login required", headers={"WWW-Authenticate": "Bearer"})
    return None


@app.get("/unencodable")
def unencodable(req):
    raise HTTPException(400, object())


@app.get("/secret")
def secret(req):
    raise RuntimeError("secret 5b1e")
