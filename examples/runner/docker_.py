"""Example script demonstrating Docker backfiller."""

import datetime as dt
import os

import dotenv
import interloper as il
from interloper_assets import DemoSource
from interloper_docker.runner import DockerRunner

dotenv.load_dotenv()


destination = il.FileDestination(base_path="/tmp/data")
source = DemoSource(destination=destination)
dag = il.DAG(source)
partition = il.TimePartition(value=dt.date(2025, 1, 1))

with DockerRunner(
    on_event=print,
    image="interloper:latest-worker",
    volumes=[f"{os.path.abspath('./data')}:/tmp/data:rw"],
) as runner:
    result = runner.run(dag, partition)
print(result)
