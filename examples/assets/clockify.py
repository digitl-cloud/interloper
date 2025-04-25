import logging

import interloper as itlp
from interloper_assets import clockify

itlp.basic_logging(logging.DEBUG)

data = clockify().holidays.run(workspace_id=itlp.Env("CLOCKIFY_WORKSPACE_ID"))

print(data)
