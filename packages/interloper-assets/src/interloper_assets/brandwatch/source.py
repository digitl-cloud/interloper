
import interloper as il

from interloper_assets.brandwatch.connection import BrandwatchConnection

# ------------------------------------------------------------------
# SOURCE
# ------------------------------------------------------------------


@il.source(
    resources={"connection": BrandwatchConnection},
    tags=["Social Media"],
    icon="fluent:connector-24-filled",
)
class Brandwatch(il.Source):
    """Brandwatch (Falcon.io) social media analytics integration."""
