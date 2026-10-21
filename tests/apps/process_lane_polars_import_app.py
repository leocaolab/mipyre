"""App for tests/test_process_lane.py: imports polars at the top and runs every route on
process lanes (`Mipyre(runtime="process")`)."""

import polars as pl

from mipyre import Mipyre

DF = pl.DataFrame({"k": [1, 1, 2, 2, 2], "v": [1.0, 2.0, 3.0, 4.0, 5.0]})

app = Mipyre(runtime="process")


@app.get("/q")
def q(req):
    g = DF.group_by("k").agg(pl.col("v").sum()).sort("k")
    return {"k": g["k"].to_list(), "v": g["v"].to_list()}
