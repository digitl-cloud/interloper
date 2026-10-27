"""Authorised application routes: list and revoke the OAuth clients acting for the caller.

An application is authorised while it holds a live refresh token for the
caller; it then acts as them in every organisation they belong to, with their
live role in each. Like personal access tokens, these routes answer the
session cookie alone, so a token an application holds cannot keep itself
alive or revoke its siblings.
"""

from __future__ import annotations

from datetime import datetime

from fastapi import APIRouter, Response
from interloper_db.models import OAuthClient
from pydantic import BaseModel

from interloper_api.dependencies import SessionUserDep, StoreDep

router = APIRouter(prefix="/auth/apps", tags=["apps"])


class AppResponse(BaseModel):
    """An application the caller has authorised; ``id`` is its OAuth client id. Never carries secret material."""

    id: str
    name: str
    redirect_uris: list[str]
    created_at: datetime | None = None

    @classmethod
    def from_client(cls, client: OAuthClient) -> AppResponse:
        """Describe a stored client.

        Args:
            client: The client row.

        Returns:
            Its public description, without the secret hash.
        """
        return cls(
            id=client.client_id,
            name=client.name,
            redirect_uris=client.redirect_uris,
            created_at=client.created_at,
        )


@router.get("")
def list_apps(user: SessionUserDep, store: StoreDep) -> list[AppResponse]:
    """List the applications the caller has authorised.

    Args:
        user: The caller, authenticated by the session cookie alone.
        store: The database store.

    Returns:
        The applications holding a live refresh token for the caller, by name.
    """
    return [AppResponse.from_client(client) for client in store.oauth.authorised_clients(user.id)]


@router.delete("/{client_id}", status_code=204)
def revoke_app(client_id: str, user: SessionUserDep, store: StoreDep) -> Response:
    """Revoke an application's access for the caller.

    Every refresh and access token it holds for the caller is revoked, and a
    code approved but not yet exchanged can no longer be. Other users who
    authorised the same application keep their grants. Idempotent: an
    application holding nothing for the caller, or unknown, gets the same 204,
    so client ids don't act as an existence oracle.

    Args:
        client_id: The application's client id.
        user: The caller, authenticated by the session cookie alone.
        store: The database store.

    Returns:
        An empty 204 response.
    """
    store.oauth.revoke_client_for_user(user.id, client_id)
    return Response(status_code=204)
