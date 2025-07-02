import datetime as dt
import logging

import interloper as itlp
from interloper_assets import amazon_ads

itlp.basic_logging(logging.DEBUG)

amazon_ads = amazon_ads(
    location="EU",
    io={"file": itlp.FileIO("data")},
)

data = amazon_ads.products_advertised_products.run(
    profile_id="1678894113015010",
    date=dt.date(2025, 2, 1),
)
print(data)

# dag = itlp.DAG(amazon_ads)
# dag.materialize(partition=itlp.TimePartition(dt.date(2025, 1, 1)))
