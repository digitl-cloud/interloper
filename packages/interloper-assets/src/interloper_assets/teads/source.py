
import interloper as il

from interloper_assets.teads.connection import TeadsConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    relations={"connection": il.Relation(TeadsConnection)},
    tags=["Advertising"],
    icon="icon:teads",
)
class Teads(il.Source):
    """Teads advertising platform integration."""
