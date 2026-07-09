"""Core-shipped OAuth providers.

Each constant is registered through interloper-core's own
``interloper.oauth_providers`` entry points (see ``pyproject.toml``) —
the same mechanism third-party packages use, so the registry loader has
no special cases.
"""

from __future__ import annotations

from interloper.oauth.base import OAuthProvider, token_params

AMAZON = OAuthProvider(
    key="amazon",
    auth_url="https://www.amazon.com/ap/oa",
    token_url="https://api.amazon.com/auth/o2/token",
    icon="icon:amazon",
)

CRITEO = OAuthProvider(
    key="criteo",
    auth_url="https://consent.criteo.com/request",
    token_url="https://api.criteo.com/oauth2/token",
)

FACEBOOK = OAuthProvider(
    key="facebook",
    auth_url="https://www.facebook.com/v19.0/dialog/oauth",
    token_url="https://graph.facebook.com/v19.0/oauth/access_token",
    icon="logos:facebook",
    token_method="get",
    token_params=token_params("grant_type"),
)

GOOGLE = OAuthProvider(
    key="google",
    auth_url="https://accounts.google.com/o/oauth2/v2/auth",
    token_url="https://oauth2.googleapis.com/token",
    icon="devicon:google",
)

LINKEDIN = OAuthProvider(
    key="linkedin",
    auth_url="https://www.linkedin.com/oauth/v2/authorization",
    token_url="https://www.linkedin.com/oauth/v2/accessToken",
    label="LinkedIn",
    icon="devicon:linkedin",
    token_encoding="form",
)

MICROSOFT = OAuthProvider(
    key="microsoft",
    auth_url="https://login.microsoftonline.com/common/oauth2/v2.0/authorize",
    token_url="https://login.microsoftonline.com/common/oauth2/v2.0/token",
    icon="logos:microsoft-icon",
    token_encoding="form",
)

PINTEREST = OAuthProvider(
    key="pinterest",
    auth_url="https://www.pinterest.com/oauth",
    token_url="https://api.pinterest.com/v5/oauth/token",
    icon="logos:pinterest",
    token_encoding="form",
    token_basic_auth=True,
)

SNAPCHAT = OAuthProvider(
    key="snapchat",
    auth_url="https://accounts.snapchat.com/login/oauth2/authorize",
    token_url="https://accounts.snapchat.com/login/oauth2/access_token",
    icon="logos:snapchat",
    token_encoding="form",
)

TIKTOK = OAuthProvider(
    key="tiktok",
    auth_url="https://business-api.tiktok.com/portal/auth",
    token_url="https://business-api.tiktok.com/open_api/v1.3/oauth2/access_token",
    label="TikTok",
    icon="logos:tiktok-icon",
    token_params=token_params(
        "grant_type",
        "redirect_uri",
        code="auth_code",
        client_id="app_id",
        client_secret="secret",
    ),
)
