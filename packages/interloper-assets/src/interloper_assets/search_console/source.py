
import interloper as il

from interloper_assets.search_console.connection import SearchConsoleConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    resources={"connection": SearchConsoleConnection},
    tags=["SEO"],
    icon="devicon:google",
)
class SearchConsole(il.Source):
    """Google Search Console integration for search analytics data."""

    site_url: str = il.InputField(description="Site URL (e.g. https://example.com/ or sc-domain:example.com)")

    def asset_table(self, asset: il.Asset) -> str:
        """Suffix tables with the site_url so instances materialize side by side."""
        return f"{asset.key}__{self.site_url}"
