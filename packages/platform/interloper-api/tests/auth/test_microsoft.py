"""Tests for ``interloper_api.auth.microsoft``: what a Microsoft provider is built from and what it accepts.

The login's flow against Microsoft runs in ``tests/routes/test_auth.py``.
"""

from __future__ import annotations

from typing import Any

import pytest

from interloper_api.auth import Account, LoginError, MicrosoftProvider
from interloper_api.settings import ApiSettings

TENANT = "0c1ea3a1-36e3-4a64-8a3a-2b3e8b0e2a51"
ISSUER = f"https://login.microsoftonline.com/{TENANT}/v2.0"


def _provider() -> MicrosoftProvider:
    return MicrosoftProvider(client_id="cid", client_secret="secret", public_url="https://app.example.com")


def _claims(**overrides: Any) -> dict[str, Any]:
    claims = {
        "iss": ISSUER,
        "tid": TENANT,
        "oid": "oid-1",
        "sub": "pairwise-1",
        "email": "ada@contoso.com",
        "xms_edov": True,
        "name": "Ada",
        **overrides,
    }
    return {name: value for name, value in claims.items() if value is not None}


class TestFromSettings:
    def test_a_configured_client_builds_a_provider(self, fake_settings: ApiSettings) -> None:
        fake_settings.auth.microsoft_client_id = "mid"
        fake_settings.auth.microsoft_client_secret = "msecret"

        provider = MicrosoftProvider.from_settings(fake_settings)

        assert provider is not None
        assert provider.name == "microsoft"
        assert provider.label == "Microsoft"

    def test_the_callback_is_served_on_the_public_url(self, fake_settings: ApiSettings) -> None:
        fake_settings.auth.microsoft_client_id = "mid"
        fake_settings.auth.microsoft_client_secret = "msecret"
        fake_settings.server.external_url = "https://app.example.com/"

        provider = MicrosoftProvider.from_settings(fake_settings)

        assert provider is not None
        assert provider.redirect_uri == "https://app.example.com/api/auth/microsoft/callback"

    @pytest.mark.parametrize("field", ["microsoft_client_id", "microsoft_client_secret"])
    def test_a_missing_client_builds_none(self, fake_settings: ApiSettings, field: str) -> None:
        fake_settings.auth.microsoft_client_id = "mid"
        fake_settings.auth.microsoft_client_secret = "msecret"
        setattr(fake_settings.auth, field, "")

        assert MicrosoftProvider.from_settings(fake_settings) is None

    def test_work_and_school_accounts_of_any_tenant_sign_in(self) -> None:
        assert _provider().metadata_url == (
            "https://login.microsoftonline.com/organizations/v2.0/.well-known/openid-configuration"
        )


class TestAccount:
    def test_the_account_is_the_object_in_its_tenant(self) -> None:
        assert _provider().account(_claims()) == Account(
            issuer=ISSUER, subject="oid-1", email="ada@contoso.com", name="Ada", avatar_url=None
        )

    def test_the_name_is_optional(self) -> None:
        assert _provider().account(_claims(name=None)).name is None

    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param(_claims(oid=None), id="no object id"),
            pytest.param(_claims(email=None), id="no email"),
            pytest.param(_claims(xms_edov=None), id="no domain verification claim"),
            pytest.param(_claims(xms_edov=False), id="unverified domain"),
            pytest.param(_claims(xms_edov="true"), id="verification claim not a boolean"),
        ],
    )
    def test_an_account_without_an_address_its_domain_owner_verified_is_refused(self, claims: dict[str, Any]) -> None:
        with pytest.raises(LoginError) as raised:
            _provider().account(claims)

        assert raised.value.reason == "login_failed"


class TestIssuer:
    """Every tenant has its own issuer, so the token's issuer must name the token's own tenant."""

    @staticmethod
    def _validate(claims: dict[str, Any]) -> bool:
        options = _provider().claims_options()
        assert options is not None
        assert options["iss"]["essential"] is True
        return options["iss"]["validate"](claims, claims.get("iss"))

    def test_the_issuer_of_the_tokens_tenant_is_accepted(self) -> None:
        assert self._validate(_claims()) is True

    @pytest.mark.parametrize(
        "claims",
        [
            pytest.param(_claims(iss="https://login.microsoftonline.com/another-tenant/v2.0"), id="another tenant"),
            pytest.param(_claims(iss="https://login.microsoftonline.com/{tenantid}/v2.0"), id="the template"),
            pytest.param(_claims(iss=f"https://sts.windows.net/{TENANT}/"), id="the v1 issuer"),
            pytest.param(_claims(tid=None), id="no tenant"),
        ],
    )
    def test_any_other_issuer_is_refused(self, claims: dict[str, Any]) -> None:
        assert self._validate(claims) is False

    @pytest.mark.parametrize("issuer", [ISSUER, "https://login.microsoftonline.com/another-tenant/v2.0"])
    def test_issues_every_tenants_issuer(self, issuer: str) -> None:
        assert _provider().issues(issuer) is True

    @pytest.mark.parametrize(
        "issuer",
        [
            "https://accounts.google.com",
            f"https://sts.windows.net/{TENANT}/",
            f"https://login.microsoftonline.com/{TENANT}/v2.0/extra",
            f"https://login.microsoftonline.com.evil.example/{TENANT}/v2.0",
        ],
    )
    def test_issues_no_other_issuer(self, issuer: str) -> None:
        assert _provider().issues(issuer) is False
