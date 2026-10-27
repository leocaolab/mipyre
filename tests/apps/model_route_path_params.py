"""App for tests/test_python_package.py::test_model_route_also_injects_path_params: `body=`
routes whose handlers also take path parameters. Its own module: a worker serves one
app per module."""

from pydantic import BaseModel

from mipyre import Mipyre


class Item(BaseModel):
    name: str


app = Mipyre(runtime="isolated")


@app.put("/items/{item_id}", body=Item)
def update(req, item: Item, item_id):
    return {"id": item_id, "name": item.name, "path": req.path}


@app.post("/tags/{tag}", body=Item)
async def tag(item: Item, tag):
    return {"tag": tag, "name": item.name}
