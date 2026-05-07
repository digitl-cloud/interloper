"""Example script demonstrating serial backfiller."""

import datetime as dt

import dotenv
import interloper as il
from interloper_assets import DemoSource

dotenv.load_dotenv()


destination = il.FileDestination(base_path="/tmp/data")
source = DemoSource(destination=destination)
dag = il.DAG(source)
partition = il.TimePartition(value=dt.date(2025, 1, 1))

with il.SerialRunner(on_event=print) as runner:
    result = runner.run(dag, partition)
print(result)
