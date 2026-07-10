
import interloper as il

from interloper_assets.usercentrics.connection import UsercentricsConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    resources={"connection": UsercentricsConnection},
    tags=["Privacy & Consent"],
    icon="fluent:connector-24-filled",
)
class Usercentrics(il.Source):
    """Usercentrics consent management analytics integration."""
