"""Example script demonstrating Docker backfiller."""

import asyncio
import datetime as dt

import dotenv
import interloper as il
from interloper_assets import DemoSource
from interloper_k8s.runner import KubernetesRunner

dotenv.load_dotenv()


destination = il.FileDestination(base_path="/tmp/data")
source = DemoSource(destination=destination)
dag = il.DAG(source)
partition = il.TimePartition(value=dt.date(2025, 1, 1))

runner = KubernetesRunner(
    on_event=print,
    image="interloper:latest-worker",
    image_pull_policy="Never",  # when running locally
    namespace="interloper",
)
result = asyncio.run(runner.run(dag, partition))
print(result)
