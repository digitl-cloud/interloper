import json
import logging
import os
from base64 import b64decode

import interloper as itlp
from interloper_assets import display_video_360

itlp.basic_logging(logging.DEBUG)

sa_key_encoded = os.environ["DISPLAY_VIDEO_360_SERVICE_ACCOUNT_KEY"]
sa_key_decoded = b64decode(sa_key_encoded).decode()
sa_key = json.loads(sa_key_decoded)

data = display_video_360(service_account_key=sa_key).partners.run()

print(data)
