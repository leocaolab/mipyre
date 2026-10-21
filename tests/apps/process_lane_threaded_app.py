"""App for tests/test_process_lane.py: runs a polars query at import (it starts polars'
thread pools in the importing process) and serves one process route."""

import polars as pl

from mipyre import Mipyre

df = pl.DataFrame({"k": [1, 1, 2], "v": [1.0, 2.0, 3.0]})
df.group_by("k").agg(pl.col("v").sum())

app = Mipyre()


@app.get("/q", runtime="process")
def q(req):
    return {"n": df.height}


if __name__ == "__main__":
    app.run(port=0)
