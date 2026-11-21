"""The sign-in link ``interloper dev`` prints, and the route that takes it.

Only the platform process ``dev`` starts mounts the route, and that process
binds ``127.0.0.1``: no deployment, ``interloper app`` included, can turn it
on. The code lives in memory for the life of the ``dev`` process; following
the link starts an ordinary session for the local developer, so the app
behaves exactly as it does after a provider's login.
"""

from __future__ import annotations

import secrets

from fastapi import APIRouter, HTTPException
from interloper_api.dependencies import SettingsDep, StoreDep
from interloper_db import Organisation, Profile, Store
from interloper_db.organisation_keys import derive_key
from pydantic import BaseModel
from starlette.responses import RedirectResponse

PATH = "/api/dev/sign-in"


class SignIn(BaseModel):
    """The code the link carries, and where it lands.

    The local developer and the organisation are looked up when the link is
    followed, not when ``dev`` starts, so a database reset under a running
    ``dev`` never leaves the route pointing at rows that are gone.

    Attributes:
        code: The secret the link carries.
        organisation: The dev organisation's name, the project's.
        extension: The key of the extension whose page the link lands on;
            ``None`` lands on the organisation.
    """

    code: str
    organisation: str
    extension: str | None = None

    @classmethod
    def generate(cls, organisation: str, extension: str | None = None) -> SignIn:
        """Draw a fresh code.

        Args:
            organisation: The dev organisation's name.
            extension: The key of the extension to land on, if any.

        Returns:
            The sign-in, its code 32 random bytes, URL-safe.
        """
        return cls(code=secrets.token_urlsafe(32), organisation=organisation, extension=extension)

    def link(self, port: int) -> str:
        """Spell the link to print and open.

        It names ``127.0.0.1``, never ``localhost``: a session cookie belongs
        to one host, and the platform binds that one.

        Args:
            port: The port the platform serves on.

        Returns:
            The URL.
        """
        return f"http://127.0.0.1:{port}{PATH}?code={self.code}"

    def ensure(self, store: Store) -> tuple[Profile, Organisation]:
        """Get the local developer and the dev organisation, creating them on first use.

        Args:
            store: The store they live in.

        Returns:
            The local developer, a super-admin, and the organisation it administers.
        """
        developer = store.profiles.local_developer()
        organisation = store.organisations.ensure(
            self.organisation, key=derive_key(self.organisation), admin_id=developer.id
        )
        return developer, organisation

    def router(self) -> APIRouter:
        """Build the route taking the link.

        Returns:
            A router answering ``GET /api/dev/sign-in``, outside the API schema.
        """
        router = APIRouter()
        router.add_api_route(PATH, self._sign_in, methods=["GET"], include_in_schema=False)
        return router

    def _sign_in(self, code: str, store: StoreDep, settings: SettingsDep) -> RedirectResponse:
        """Start a session for the local developer when the code is this process's.

        Args:
            code: The code the link carries.
            store: The store sessions are written through.
            settings: The API's settings, the session cookie's among them.

        Returns:
            A redirect to the extension's page or the organisation, the
            session cookie set.

        Raises:
            HTTPException: 403 when the code is not the one ``dev`` printed.
        """
        if not secrets.compare_digest(code.encode(), self.code.encode()):
            raise HTTPException(status_code=403, detail="This is not the sign-in link interloper dev printed")
        developer, organisation = self.ensure(store)
        landing = f"/{organisation.key}/ext/{self.extension}" if self.extension else f"/{organisation.key}"
        response = RedirectResponse(url=landing, status_code=302)
        settings.auth.set_session_cookie(response, store.tokens.create_session(developer.id))
        return response
