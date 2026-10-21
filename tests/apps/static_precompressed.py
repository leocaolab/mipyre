"""Precompressed static app for tests/test_static_precompressed.py. The suite creates the
root and exports it as MIPYRE_TEST_PRECOMPRESSED_DIR before importing this module;
workers inherit the environment, so they mount the same root."""

import os

from mipyre import Mipyre

app = Mipyre()

app.static("/static", os.environ["MIPYRE_TEST_PRECOMPRESSED_DIR"], precompressed=True)
