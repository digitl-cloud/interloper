import logging

from google.auth import default
from google.auth.credentials import Credentials as GoogleCredentials
from google.auth.impersonated_credentials import Credentials as ImpersonatedCredentials
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials as GoogleOAuth2Credentials
from google.oauth2.service_account import Credentials as ServiceAccountCredentials
from interloper.rest.auth import Auth

logger = logging.getLogger(__name__)


class GoogleAuth(Auth):
    def __init__(
        self,
        service_account_key: dict[str, str] | None = None,
        impersonated_account: str | None = None,
        scopes: list[str] | None = None,
    ):
        super().__init__()
        self._service_account_key = service_account_key
        self._impersonated_account = impersonated_account
        self._scopes = scopes

    def __call__(self) -> GoogleCredentials:
        if self._service_account_key is not None:
            logger.debug("Using service account key for authentication")
            credentials = ServiceAccountCredentials.from_service_account_info(
                self._service_account_key,
                scopes=self._scopes,
            )
            logger.debug(f"Credentials created from service account key ({credentials.service_account_email})")
        else:
            credentials, _ = default(scopes=self._scopes)

        if self._impersonated_account is not None:
            target_credentials = ImpersonatedCredentials(
                source_credentials=credentials,
                target_principal=self._impersonated_account,
                delegates=[],
                target_scopes=self._scopes,
                lifetime=300,
            )
            request = Request()
            target_credentials.refresh(request)
            credentials = target_credentials

        self.authenticated = True
        return credentials  # type: ignore # TODO: fix typing


class GoogleOauth2ClientCredentialsAuth(GoogleAuth):
    def __init__(
        self,
        client_id: str,
        client_secret: str,
        refresh_token: str,
        token_uri: str = "https://accounts.google.com/o/oauth2/token",
        scopes: list[str] | None = None,
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._refresh_token = refresh_token
        self._token_uri = token_uri
        self._scopes = scopes

    def __call__(self) -> GoogleOAuth2Credentials:
        credentials = GoogleOAuth2Credentials(
            None,
            client_id=self._client_id,
            client_secret=self._client_secret,
            refresh_token=self._refresh_token,
            token_uri=self._token_uri,
        )
        self.authenticated = True
        return credentials
