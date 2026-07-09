"""Tests for the built-in OAuth provider specs."""

from interloper.oauth import providers


class TestBuiltinProviders:
    def test_labels(self):
        assert providers.LINKEDIN.label == "LinkedIn"
        assert providers.TIKTOK.label == "TikTok"
        assert providers.AMAZON.label == "Amazon"

    def test_facebook_exchanges_via_get_without_grant_type(self):
        assert providers.FACEBOOK.token_method == "get"
        assert "grant_type" not in providers.FACEBOOK.token_params

    def test_tiktok_renames_and_omits_params(self):
        assert providers.TIKTOK.token_params == {
            "code": "auth_code",
            "client_id": "app_id",
            "client_secret": "secret",
        }

    def test_pinterest_uses_basic_auth(self):
        assert providers.PINTEREST.token_basic_auth is True

    def test_form_encoded_providers(self):
        for spec in (providers.LINKEDIN, providers.MICROSOFT, providers.PINTEREST, providers.SNAPCHAT):
            assert spec.token_encoding == "form"
