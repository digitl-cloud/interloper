import datetime as dt

import interloper as il
from dotenv import load_dotenv
from interloper_assets import DemoSource

load_dotenv()

il.EventBus.subscribe(print)

mem = il.MemoryDestination()
demo = DemoSource(destination=mem)


@il.asset()
def XXX() -> str:
    print("x")
    return "x"


x = XXX(id="xxx", destination=mem)
demo.b.deps["x"] = x.id

dag = il.DAG(demo, x)
partition = il.TimePartition(dt.date(2024, 1, 1))
result = dag.materialize(partition_or_window=partition)
# print(result)

# result = demo.b.run(partition_or_window=partition, dag=dag)
# print(result)
