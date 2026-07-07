"""Example script demonstrating Docker backfiller."""

import asyncio
import datetime as dt
import os

import dotenv
import interloper as il
from interloper_assets import DemoSource
from interloper_docker.runner import DockerRunner

dotenv.load_dotenv()


destination = il.FileDestination(base_path="/tmp/data")
source = DemoSource(destinations=[destination])
dag = il.DAG(source)
partition = il.TimePartition(value=dt.date(2025, 1, 1))

runner = DockerRunner(
    on_event=print,
    image="interloper:latest-worker",
    volumes=[f"{os.path.abspath('./data')}:/tmp/data:rw"],
)
result = asyncio.run(runner.run(dag, partition))
print(result)
