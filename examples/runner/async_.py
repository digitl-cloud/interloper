"""Example script demonstrating async backfiller."""

import asyncio
import datetime as dt

import dotenv
import interloper as il
from interloper_assets import DemoSource

dotenv.load_dotenv()


destination = il.FileDestination(base_path="/tmp/data")
source = DemoSource(destination=destination)
dag = il.DAG(source)
partition = il.TimePartition(value=dt.date(2025, 1, 1))


async def run() -> None:
    result = await il.AsyncRunner(max_workers=2, on_event=print).run(dag, partition)
    print(result)


if __name__ == "__main__":
    asyncio.run(run())
