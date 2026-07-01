
import interloper as il
from pydantic_settings import SettingsConfigDict


@il.connection(
    name="Search Console",
    icon="devicon:google",
    tags=["SEO"],
)
class SearchConsoleConnection(il.Connection):
    """Google Search Console API connection with service account auth."""

    model_config = SettingsConfigDict(env_prefix="search_console_")

    service_account_key: str = il.JsonField(description="Google service account key JSON")
