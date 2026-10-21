"""Stands in for a library a sub-interpreter can't load (numpy, polars...): importing it in
a sub-interpreter raises. Not numpy itself: on macOS numpy 2.5.x fails to import in any
process that has forked (b/32), which would make these tests about that bug instead."""

from mipyre.engine import _in_worker

if _in_worker():
    raise ImportError("heavy can't load in a sub-interpreter (test stand-in)")


def mean(values):
    return sum(values) / len(values)
