"""Example script demonstrating serial backfiller."""

import asyncio
import datetime as dt

import dotenv
import interloper as il
from interloper_assets import DemoSource

dotenv.load_dotenv()


destination = il.FileDestination(base_path="/tmp/data")
source = DemoSource(destinations=[destination])
dag = il.DAG(source)
partition = il.TimePartition(value=dt.date(2025, 1, 1))

result = asyncio.run(il.SerialRunner(on_event=print).run(dag, partition))
print(result)
