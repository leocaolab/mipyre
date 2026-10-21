"""App for tests/test_openapi.py: a `model=` route, `query=` routes (alone and with a body), path
params, a catch-all, each kind of return annotation, a fast route and an MCP tool, with the OpenAPI
document enabled. Its own module: a
worker serves one app per module."""

from __future__ import annotations

from typing import Literal

import pydantic

from mipyre import Mipyre


class Tag(pydantic.BaseModel):
    label: str


class Item(pydantic.BaseModel):
    name: str
    # Bounded: the handler echoes `qty`, and the engine answers an int past 64 bits with a 500 (b/760),
    # which Schemathesis finds at 200 cases per operation.
    qty: int = pydantic.Field(ge=0, le=2**63 - 1)
    tags: list[Tag] = []


class Search(pydantic.BaseModel):
    q: str
    page: int = pydantic.Field(1, ge=1, le=1000)
    tag: list[str] = []
    kind: Literal["book", "film"] | None = None


class Dry(pydantic.BaseModel):
    dry_run: bool = False


app = Mipyre(runtime="main")
app.enable_openapi(title="Items", version="1.0.0")
app.add_fast_response("GET", "/ping", b"pong")


@app.post("/items", model=Item)
def create(item: Item) -> dict[str, int]:
    """Create an item and return its quantity."""
    return {"qty": item.qty}


@app.get("/items/{item_id}")
def show(req, item_id: str) -> dict[str, str]:
    return {"id": item_id}


@app.get("/search", query=Search)
def search(req, params: Search) -> dict[str, int]:
    """Search; `tag` repeats to ask for several."""
    return {"page": params.page, "tags": len(params.tag)}


@app.post("/items/{item_id}/tags", model=Tag, query=Dry)
def add_tag(tag: Tag, dry: Dry, item_id: str) -> dict[str, bool]:
    return {"added": not dry.dry_run}


@app.get("/files/{*rest}")
def file(req, rest: str) -> str:
    return rest


@app.get("/raw")
def raw(req) -> bytes:
    return b"\x00\x01"


@app.get("/plain")
def plain(req):
    return "no annotation"


@app.mcp.tool()
def add(a: int, b: int) -> int:
    return a + b
