import datetime as dt
import logging

import interloper as itlp
from interloper_assets import bing_ads

itlp.basic_logging(logging.DEBUG)


data = bing_ads.ads.run(date=dt.date(2025, 1, 1))

print(data)
