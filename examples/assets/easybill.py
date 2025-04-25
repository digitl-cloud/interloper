import logging

import interloper as itlp
from interloper_assets import easybill

itlp.basic_logging(logging.DEBUG)

data = easybill.customers.run()

print(data)
