from pprint import pp

import interloper as il
from interloper_assets import DemoSource

catalog = il.Catalog.from_assets(sources_or_assets=[DemoSource])

pp(catalog)
