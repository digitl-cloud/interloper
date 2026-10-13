import datetime as dt
from pprint import pp

import interloper as il
from dotenv import load_dotenv
from interloper_assets import Adup, AdupConnection

load_dotenv()

il.EventBus.subscribe(print)

adup = Adup()
dag = il.DAG(adup)

results = dag.materialize(il.TimePartition(dt.date(2024, 1, 1)))
print(results)


pp(Adup.definition().model_dump(mode="json"))
pp(Adup.definition().assets[0].model_dump(mode="json"))
pp(AdupConnection.definition().model_dump(mode="json"))
