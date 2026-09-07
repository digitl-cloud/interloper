
import interloper as il

from interloper_assets.search_console.connection import SearchConsoleConnection

# -- SOURCE --------------------------------------------------------------------


@il.source(
    tags=["SEO"],
    icon="devicon:google",
)
class SearchConsole(il.Source):
    """Google Search Console integration for search analytics data."""

    connection: SearchConsoleConnection

    site_url: str = il.InputField(
        description="Site URL (e.g. https://example.com/ or sc-domain:example.com)",
        discriminator=True,
    )
