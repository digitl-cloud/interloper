
import interloper as il

from interloper_assets.teads.connection import TeadsConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    tags=["Advertising"],
    icon="icon:teads",
)
class Teads(il.Source):
    """Teads advertising platform integration."""

    connection: TeadsConnection
