import datetime as dt
import logging

import interloper as itlp
from interloper_assets import facebook_ads

itlp.basic_logging(logging.DEBUG)

data = facebook_ads.ads.run(
    account_id=itlp.Env("FACEBOOK_ADS_ACCOUNT_ID"),
    date=dt.date(2025, 1, 1),
)

print(data)
