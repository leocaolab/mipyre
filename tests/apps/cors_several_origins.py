"""App for tests/test_cors_404.py::test_of_several_origins_only_the_request_s_own_is_named.
Its own module: a worker serves one app per module."""

from mipyre import Mipyre

app = Mipyre()
app.enable_cors(
    allow_origins=["https://a.example", "https://b.example"],
    allow_credentials=True,
)


@app.get("/")
def root(req):
    return "hello"
