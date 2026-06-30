"""Per-connection OAuth configuration."""

from __future__ import annotations

from typing import Any

from interloper.errors import ConfigError
from interloper.oauth.base import providers

#: Default ``fields`` mapping — the standard credential trio, named identically.
_DEFAULT_FIELDS = {
    "client_id": "client_id",
    "client_secret": "client_secret",
    "refresh_token": "refresh_token",
}


class OAuthConfig:
    """Class-level OAuth configuration for connection resources.

    Declares that a connection supports OAuth sign-in: which provider to
    use and what scope to request. Provider identity (auth_url, label, icon)
    is resolved from the provider registry; pass them explicitly only to
    override the registry or to use an unregistered provider.

    When present on a connection class (via the ``@connection(oauth=...)``
    decorator kwarg or an ``oauth`` ClassVar), ``definition()`` injects an
    ``x-oauth`` extension into the root of the generated JSON Schema so the
    frontend can render a "Sign in with X" button.

    ``fields`` maps the canonical OAuth roles — ``client_id`` / ``client_secret``
    / ``refresh_token`` — to the connection's actual field names. It drives the
    whole form: the mapped fields are hidden in sign-in mode, the ``client_id``
    / ``client_secret`` fields are resolved from ``<PROVIDER>_CLIENT_ID`` /
    ``<PROVIDER>_CLIENT_SECRET`` env, and the ``refresh_token`` field receives
    the token from the sign-in response. It defaults to the identity trio, so a
    standard connection declares nothing; a connection with differently named
    fields (or only some roles) passes its own mapping.

    Args:
        provider: OAuth provider key (e.g. ``"amazon"``, ``"facebook"``).
        scope: OAuth scope string to request.
        fields: ``{role: model_field_name}`` mapping (roles: ``client_id`` /
            ``client_secret`` / ``refresh_token``). Defaults to the identity
            trio.
        auth_url: Authorization endpoint override.  Required when the
            provider is not in the registry.
        label: Display label override.
        icon: Icon identifier override.

    Raises:
        ConfigError: If the provider is unregistered and no ``auth_url``
            is given.

    Example::

        @connection(
            name="Facebook Ads",
            oauth=OAuthConfig(
                "facebook",
                scope="ads_read",
                fields={"client_id": "app_id", "client_secret": "app_secret", "refresh_token": "access_token"},
            ),
        )
        class FacebookAdsConnection(OAuthConnection):
            ...
    """

    def __init__(
        self,
        provider: str,
        *,
        scope: str = "",
        fields: dict[str, str] | None = None,
        auth_url: str = "",
        label: str = "",
        icon: str = "",
    ) -> None:
        """Initialise an OAuthConfig, resolving provider metadata from the registry.

        Args:
            provider: OAuth provider key (e.g. ``"amazon"``).
            scope: OAuth scope string.
            fields: Role → field-name mapping (see class docstring); defaults
                to the identity trio.
            auth_url: Authorization endpoint override (required for
                unregistered providers).
            label: Display label override.
            icon: Icon identifier override.

        Raises:
            ConfigError: If the provider is unregistered and no ``auth_url``
                is given.
        """
        spec = providers().get(provider)
        self.provider = provider
        self.auth_url = auth_url or (spec.auth_url if spec else "")
        if not self.auth_url:
            raise ConfigError(
                f"Unknown OAuth provider: {provider!r} (available: {sorted(providers())}). "
                f"Declare it under the 'interloper.oauth_providers' entry-point group "
                f"or pass auth_url explicitly."
            )
        self.scope = scope
        self.label = label or (spec.label if spec else "") or provider.title()
        self.icon = icon or (spec.icon if spec else "")
        self.fields = fields if fields is not None else dict(_DEFAULT_FIELDS)

    def to_schema_ext(self) -> dict[str, Any]:
        """Serialize to a JSON Schema ``x-oauth`` extension dict.

        Returns:
            Dict with provider, auth_url, scope, label, icon, and fields.
        """
        return {
            "provider": self.provider,
            "auth_url": self.auth_url,
            "scope": self.scope,
            "label": self.label,
            "icon": self.icon,
            "fields": self.fields,
        }
