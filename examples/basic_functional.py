import logging
from collections.abc import Sequence

from interloper.core.asset import Asset, asset
from interloper.core.io import FileIO
from interloper.core.pipeline import Pipeline
from interloper.core.source import source
from interloper.core.utils import basic_logging

basic_logging(logging.INFO)


@source
def my_source() -> Sequence[Asset]:
    @asset
    def my_asset_A() -> str:
        return "A"

    @asset
    def my_asset_B() -> str:
        return "B"

    return (my_asset_A, my_asset_B)


my_source.my_asset_A.run()