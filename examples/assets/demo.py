import datetime as dt

import interloper as il
from dotenv import load_dotenv
from interloper_assets import DemoSource

load_dotenv()

il.EventBus.subscribe(print)

demo = DemoSource(destinations=[il.MemoryDestination()])
dag = il.DAG(demo)

result = dag.materialize(il.TimePartition(dt.date(2024, 1, 1)))
print(result)
