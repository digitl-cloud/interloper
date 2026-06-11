"""Tests for the built-in OAuth provider specs."""

from interloper.oauth import builtin


class TestBuiltinProviders:
    def test_labels(self):
        assert builtin.LINKEDIN.label == "LinkedIn"
        assert builtin.TIKTOK.label == "TikTok"
        assert builtin.AMAZON.label == "Amazon"

    def test_facebook_exchanges_via_get_without_grant_type(self):
        assert builtin.FACEBOOK.token_method == "get"
        assert "grant_type" not in builtin.FACEBOOK.token_params

    def test_tiktok_renames_and_omits_params(self):
        assert builtin.TIKTOK.token_params == {
            "code": "auth_code",
            "client_id": "app_id",
            "client_secret": "secret",
        }

    def test_pinterest_uses_basic_auth(self):
        assert builtin.PINTEREST.token_basic_auth is True

    def test_form_encoded_providers(self):
        for spec in (builtin.LINKEDIN, builtin.MICROSOFT, builtin.PINTEREST, builtin.SNAPCHAT):
            assert spec.token_encoding == "form"
