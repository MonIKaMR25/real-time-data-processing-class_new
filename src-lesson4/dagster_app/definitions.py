"""Dagster code location: bundle the assets into a Definitions object.

`dagster dev -m dagster_app` imports the package and finds the top-level `defs`
(re-exported from __init__.py).
"""

from dagster import Definitions

from dagster_app.assets import daily_revenue, raw_daily_orders

defs = Definitions(assets=[raw_daily_orders, daily_revenue])
