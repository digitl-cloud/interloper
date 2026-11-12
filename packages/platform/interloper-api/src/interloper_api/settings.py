"""The API's settings sections, and the bundle of every section the API reads.

The API reads its own sections (``auth``, ``smtp``), the database package's
(``encryption``, ``quota``), the deployment's ``server`` and, when the agent
package is installed, ``agent``. `ApiSettings` carries them from the command
that reads them to the application and its routes.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import timedelta
from typing import TYPE_CHECKING, Annotated, Any, ClassVar

from interloper.settings import ServerSettings, Settings
from interloper_db.settings import EncryptionSettings, QuotaSettings
from pydantic import Field, field_validator
from pydantic_settings import NoDecode

if TYPE_CHECKING:
    from interloper_agent.settings import AgentSettings


class AuthSettings(Settings):
    """Authentication settings: Google OAuth and the session cookie.

    The list fields take a comma-separated string from the environment
    (``INTERLOPER_AUTH_SUPER_ADMIN_EMAILS=a@x.com,b@x.com``) or a list from
    YAML.
    """

    public: ClassVar[frozenset[str]] = frozenset(
        {
            "google_redirect_uri",
            "cookie_secure",
            "session_expiry_days",
            "access_token_expiry_minutes",
            "refresh_token_expiry_days",
            "super_admin_emails",
            "allowed_domains",
        }
    )

    google_client_id: str = Field(default="", description="Google OAuth web client id.")
    google_client_secret: str = Field(default="", description="Google OAuth web client secret.")
    google_redirect_uri: str = Field(
        default="", description="Redirect URI registered on the Google OAuth client (`/api/auth/google/callback`)."
    )
    cookie_secure: bool = Field(default=True, description="Send the session cookie over HTTPS only.")
    session_expiry_days: int = Field(default=30, description="Days a session stays valid.")
    access_token_expiry_minutes: int = Field(default=60, description="Minutes an OAuth access token stays valid.")
    refresh_token_expiry_days: int = Field(
        default=30, description="Days an OAuth refresh token stays valid, counted again from each rotation."
    )
    super_admin_emails: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="Google emails promoted to platform-wide super-admin when the API starts and on login; "
        "promotion only, removing an email never demotes.",
    )
    allowed_domains: Annotated[list[str], NoDecode] = Field(
        default_factory=list,
        description="Email domains allowed to sign up; empty keeps signup open. Configured super-admins, pending "
        "invitations and existing profiles always pass.",
    )

    @field_validator("super_admin_emails", "allowed_domains", mode="before")
    @classmethod
    def _parse_comma_list(cls, value: Any) -> list[str]:
        """Accept a comma-separated string (env) or a list (YAML).

        Args:
            value: The raw value: a comma-separated string from env, or an
                iterable of entries from YAML.

        Returns:
            Trimmed, lowercased entries with empties dropped and any leading
            ``@`` stripped (so ``@example.com`` and ``example.com`` both work).
        """
        if isinstance(value, str):
            value = value.split(",")
        return [entry.strip().lower().lstrip("@") for entry in value if entry and entry.strip()]

    @property
    def session_expiry(self) -> timedelta:
        """How long a login session stays valid.

        Returns:
            ``session_expiry_days`` as a duration.
        """
        return timedelta(days=self.session_expiry_days)

    @property
    def access_token_expiry(self) -> timedelta:
        """How long an OAuth access token stays valid.

        Returns:
            ``access_token_expiry_minutes`` as a duration.
        """
        return timedelta(minutes=self.access_token_expiry_minutes)

    @property
    def refresh_token_expiry(self) -> timedelta:
        """How long an OAuth refresh token stays valid, counted again from each rotation.

        Returns:
            ``refresh_token_expiry_days`` as a duration.
        """
        return timedelta(days=self.refresh_token_expiry_days)


class SmtpSettings(Settings):
    """SMTP settings for sending invitation emails."""

    public: ClassVar[frozenset[str]] = frozenset({"host", "port", "from_addr"})

    host: str = Field(default="", description="SMTP host; sending is enabled once host, user and password are set.")
    port: int = Field(default=587, description="SMTP port.")
    user: str = Field(default="", description="SMTP user.")
    password: str = Field(default="", description="SMTP password.")
    from_addr: str = Field(default="noreply@interloper.dev", description="Sender address of invitation emails.")

    @property
    def enabled(self) -> bool:
        """SMTP is enabled when host, user, and password are all set."""
        return bool(self.host and self.user and self.password)


@dataclass(frozen=True)
class ApiSettings:
    """Every section the API reads, as the command serving it read them.

    ``agent`` is ``None`` when the agent package is not installed.
    """

    server: ServerSettings
    auth: AuthSettings
    smtp: SmtpSettings
    encryption: EncryptionSettings
    quota: QuotaSettings
    agent: AgentSettings | None = None

    @classmethod
    def get(cls) -> ApiSettings:
        """Read each section the way this process runs with it.

        Returns:
            The sections, ``agent`` included when its package is installed.
        """
        try:
            from interloper_agent.settings import AgentSettings
        except ImportError:
            agent = None
        else:
            agent = AgentSettings.get()
        return cls(
            server=ServerSettings.get(),
            auth=AuthSettings.get(),
            smtp=SmtpSettings.get(),
            encryption=EncryptionSettings.get(),
            quota=QuotaSettings.get(),
            agent=agent,
        )
