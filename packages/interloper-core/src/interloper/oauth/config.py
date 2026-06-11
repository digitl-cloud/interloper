"""Per-connection OAuth configuration."""

from __future__ import annotations

from typing import Any

from interloper.errors import ConfigError
from interloper.oauth.base import providers


class OAuthConfig:
    """Class-level OAuth configuration for connection resources.

    Declares that a connection supports OAuth sign-in: which provider to
    use, what scope to request, and how to map the token response back to
    model field names.  Provider identity (auth_url, label, icon) is
    resolved from the provider registry; pass them explicitly only to
    override the registry or to use an unregistered provider.

    When present on a connection class (via the ``@connection(oauth=...)``
    decorator kwarg or an ``oauth`` ClassVar), ``definition()`` injects an
    ``x-oauth`` extension into the root of the generated JSON Schema so the
    frontend can render a "Sign in with X" button.

    Args:
        provider: OAuth provider key (e.g. ``"amazon"``, ``"facebook"``).
        scope: OAuth scope string to request.
        fields: Mapping of ``{token_response_key: model_field_name}``.
            Defaults to the standard trio: client_id, client_secret,
            refresh_token (the fields ``OAuthConnection`` declares).
        auth_url: Authorization endpoint override.  Required when the
            provider is not in the registry.
        label: Display label override.
        icon: Icon identifier override.

    Raises:
        ConfigError: If the provider is unregistered and no ``auth_url``
            is given.

    Example::

        @connection(
            name="Amazon Ads",
            oauth=OAuthConfig("amazon", scope="advertising::campaign_management"),
        )
        class AmazonAdsConnection(OAuthConnection):
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
            fields: Token response key → model field name mapping.
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
        self.fields = fields or {
            "client_id": "client_id",
            "client_secret": "client_secret",
            "refresh_token": "refresh_token",
        }

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
