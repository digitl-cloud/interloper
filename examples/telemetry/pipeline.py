"""Telemetry demo: a DAG whose shape is designed to be read in Grafana.

Run it against the local stack in this directory (see README.md)::

    docker compose -f examples/telemetry/docker-compose.yml up -d
    uv run python examples/telemetry/pipeline.py --runs 20

Telemetry is configured the way any embedding application would do it —
through ``TelemetrySettings`` and ``init_telemetry``, not by assembling
the OpenTelemetry SDK by hand.
"""

# No ``from __future__ import annotations``: sibling-dependency inference
# resolves upstream assets from real annotations, not lazy strings.

import argparse
import datetime as dt
import logging
import random
import time
from typing import Any

import interloper as il
from interloper.settings import TelemetrySettings
from interloper.telemetry import init_telemetry, shutdown_telemetry

logger = logging.getLogger("telemetry-example")

# Every asset below is deliberately different, so one run exercises the whole
# span catalogue rather than the same two spans five times over:
#
#   customers   declared schema        → interloper.conformer.reconcile
#   orders      no schema (AUTO)       → interloper.asset.infer_schema
#   refunds     normalizer configured  → interloper.normalizer.normalize
#   fx_rates    fails ~25% of runs     → error spans + failed-status metrics
#   report      reads three upstreams  → three interloper.destination.read spans


class Customer(il.Schema):
    id: int
    name: str


class FxApi(il.Resource):
    """Stand-in for a credentialed client; resolving it is its own span."""

    base_url: str = "https://fx.example.invalid"


@il.source
def shop() -> list[type[il.Asset]]:
    @il.asset(schema=Customer)
    def customers() -> list[dict[str, Any]]:
        time.sleep(random.uniform(0.02, 0.12))
        return [{"id": i, "name": f"customer-{i}"} for i in range(1, 6)]

    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def orders(context: il.ExecutionContext, customers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        time.sleep(random.uniform(0.05, 0.3))
        return [
            {
                "order_id": 1000 + c["id"],
                "customer_id": c["id"],
                "amount": 10.0 * c["id"],
                "date": context.partition_date,
            }
            for c in customers
        ]

    # A normalizer means an interloper.normalizer.normalize span appears for
    # this asset and no other — the span tree shows per-asset configuration.
    @il.asset(normalizer=il.Normalizer(), partitioning=il.TimePartitionConfig(column="date"))
    def refunds(context: il.ExecutionContext, customers: list[dict[str, Any]]) -> list[dict[str, Any]]:
        time.sleep(random.uniform(0.05, 0.2))
        return [{"refundId": 900 + c["id"], "customerId": c["id"], "date": context.partition_date} for c in customers]

    @il.asset(resources={"fx_api": FxApi}, partitioning=il.TimePartitionConfig(column="date"))
    def fx_rates(context: il.ExecutionContext, fx_api: FxApi) -> list[dict[str, Any]]:
        time.sleep(random.uniform(0.05, 0.4))
        # The interesting Grafana panels are the ones with failures in them.
        if random.random() < 0.25:
            raise RuntimeError("FX provider returned 503")
        return [{"currency": "EUR", "rate": 1.08, "date": context.partition_date}]

    @il.asset(partitioning=il.TimePartitionConfig(column="date"))
    def report(
        context: il.ExecutionContext,
        orders: list[dict[str, Any]],
        refunds: list[dict[str, Any]],
        fx_rates: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        time.sleep(random.uniform(0.1, 0.4))
        rate = fx_rates[0]["rate"] if fx_rates else 1.0
        return [
            {
                "orders": len(orders),
                "refunds": len(refunds),
                "revenue_eur": round(sum(o["amount"] for o in orders) * rate, 2),
                "date": context.partition_date,
            }
        ]

    return [customers, orders, refunds, fx_rates, report]


def main() -> None:
    """Run the demo pipeline and export its telemetry."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runs", type=int, default=0, help="Number of runs; 0 loops until Ctrl-C (default)")
    parser.add_argument("--endpoint", default="http://localhost:4317", help="OTLP endpoint")
    parser.add_argument("--interval", type=float, default=2.0, help="Seconds between runs")
    parser.add_argument("--data-dir", default="/tmp/interloper-telemetry", help="Where assets are written")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")

    if not init_telemetry(
        TelemetrySettings(
            enabled=True,
            endpoint=args.endpoint,
            service_name="interloper-example",
            # Faster than the framework default so a short demo still gives the
            # rate-based panels several points to work with.
            metric_export_interval=5,
        ),
        role="example",
    ):
        raise SystemExit("Telemetry did not start. Install the extra: uv sync --all-extras")

    # Destinations bind at instantiation; upstream assets are read back from
    # them, which is what produces the destination.read spans.
    dag = il.DAG(shop(destinations=[il.FileDestination(base_path=args.data_dir)]))
    # A few days of partitions so the traces aren't all identical.
    dates = [dt.date(2026, 3, 1) + dt.timedelta(days=i) for i in range(5)]

    logger.info("Sending telemetry to %s — dashboard at http://localhost:8080", args.endpoint)
    if not args.runs:
        logger.info("Looping until Ctrl-C; the dashboard fills in as runs complete.")

    completed = 0
    try:
        while not args.runs or completed < args.runs:
            partition = il.TimePartition(value=random.choice(dates))
            # The DAG entrypoint, rather than driving a runner directly — it
            # adds the interloper.dag.materialize span at the top of the trace.
            result = dag.materialize(partition)
            completed += 1
            logger.info("run %d — %s", completed, result)
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Stopping after %d run(s)", completed)
    finally:
        # Flushes both providers; without it the last run's telemetry is lost.
        shutdown_telemetry()

    logger.info("Done. Open Grafana at http://localhost:8080")


if __name__ == "__main__":
    main()
