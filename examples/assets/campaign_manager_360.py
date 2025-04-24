import datetime as dt
import json
import logging
import os
from base64 import b64decode

import interloper as itlp
from interloper_assets import campaign_manager_360

itlp.basic_logging(logging.DEBUG)

sa_key_encoded = os.environ["CAMPAIGN_MANAGER_360_SERVICE_ACCOUNT_KEY"]
sa_key_decoded = b64decode(sa_key_encoded).decode()
sa_key = json.loads(sa_key_decoded)

data = campaign_manager_360(service_account_key=sa_key).ads.run(
    date=dt.date(2025, 1, 1),
    profile_id=itlp.Env("CAMPAIGN_MANAGER_360_PROFILE_ID"),
    account_id=itlp.Env("CAMPAIGN_MANAGER_360_ACCOUNT_ID"),
)

print(data)
