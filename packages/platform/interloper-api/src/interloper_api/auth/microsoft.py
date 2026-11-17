"""Sign in with a Microsoft work or school account."""

from __future__ import annotations

import logging
import re
from typing import TYPE_CHECKING, Any

from interloper_api.auth.provider import Account, LoginError, LoginProvider

if TYPE_CHECKING:
    from interloper_api.settings import ApiSettings

logger = logging.getLogger(__name__)

AUTHORITY = "https://login.microsoftonline.com"


class MicrosoftProvider(LoginProvider):
    """Microsoft Entra ID, for the work and school accounts of every tenant.

    Each tenant issues its own tokens: the issuer is
    ``https://login.microsoftonline.com/<tenant id>/v2.0``, which the
    multi-tenant discovery document can only name as a template, so a token's
    issuer is checked against its own ``tid``. Within its tenant, an account is
    named by its object id, ``oid``, for its whole life.

    Entra can put an address nobody verified in ``email`` (the nOAuth
    takeover), so the address is trusted only when ``xms_edov`` says the
    owner of its domain is verified; the app registration must emit both as
    optional claims. An ID token carries no picture.
    """

    name = "microsoft"
    label = "Microsoft"
    metadata_url = f"{AUTHORITY}/organizations/v2.0/.well-known/openid-configuration"

    _TENANT_ISSUER = re.compile(rf"{re.escape(AUTHORITY)}/[^/]+/v2\.0")

    @classmethod
    def from_settings(cls, settings: ApiSettings) -> MicrosoftProvider | None:
        """Build the provider from the deployment's Microsoft Entra app registration.

        Args:
            settings: The API's settings, whose ``auth`` holds the client and
                whose ``server`` the public URL the callback is served on.

        Returns:
            The provider, or ``None`` when no Microsoft client is configured.
        """
        auth = settings.auth
        if not auth.microsoft_client_id or not auth.microsoft_client_secret:
            return None
        return cls(
            client_id=auth.microsoft_client_id,
            client_secret=auth.microsoft_client_secret,
            public_url=settings.server.public_url,
        )

    def issues(self, issuer: str) -> bool:
        """Tell whether an identity's issuer is a Microsoft tenant.

        Args:
            issuer: The issuer an identity was recorded under.

        Returns:
            Whether it is a tenant's issuer.
        """
        return self._TENANT_ISSUER.fullmatch(issuer) is not None

    def claims_options(self) -> dict[str, Any]:
        """Require the token's issuer to be its own tenant's.

        Returns:
            Authlib's claims options for the ``iss`` claim.
        """
        return {"iss": {"essential": True, "validate": self._issued_by_its_tenant}}

    def account(self, claims: dict[str, Any]) -> Account:
        """Read the Microsoft account a verified ID token proves.

        Args:
            claims: The verified ID token's claims.

        Returns:
            The account, named by its object id under its tenant's issuer.

        Raises:
            LoginError: ``login_failed`` when the token names no object or
                address, or does not say the domain's owner is verified.
        """
        if not claims.get("oid") or not claims.get("email") or claims.get("xms_edov") is not True:
            logger.warning("Microsoft login refused: the ID token names no account or no domain-verified address")
            raise LoginError("login_failed")
        return Account(
            issuer=claims["iss"],
            subject=claims["oid"],
            email=claims["email"],
            name=claims.get("name"),
            avatar_url=None,
        )

    @staticmethod
    def _issued_by_its_tenant(claims: dict[str, Any], issuer: str | None) -> bool:
        """Tell whether a token's issuer is the tenant the token names.

        Args:
            claims: The token's claims, holding its tenant as ``tid``.
            issuer: The token's ``iss``.

        Returns:
            Whether the issuer is that tenant's.
        """
        tenant = claims.get("tid")
        return bool(tenant) and issuer == f"{AUTHORITY}/{tenant}/v2.0"
