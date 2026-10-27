"""OAuth persistence: clients, authorization codes and refresh tokens.

The authorization server's storage. Every secret (client secret, code,
refresh token) is generated here, returned raw once, and stored as its
SHA-256 hash only.

Consuming a code and rotating a refresh token each run in one transaction
holding a row lock (``SELECT … FOR UPDATE`` on Postgres), so under concurrent
requests a code is exchanged once and a refresh token rotates once. Replaying
either, once spent, is read as theft: the family it belongs to is revoked.

Access tokens are ``tokens`` rows and carry no family. Revoking a family
revokes the access tokens of its user and client, which can end a sibling
family's access token early; that family's next refresh issues a new one.
A family stays revoked: a refresh token saved into it after the revocation
(the loser of a race on one token) never rotates and never counts as live,
and a pair issued into it is revoked on the spot. Revoking a family is done
once: a later replay into it, or of a code past its lifetime, carries no new
sign of theft and revokes nothing, so it cannot end a newer grant's access.
"""

from __future__ import annotations

import secrets
from datetime import datetime, timedelta, timezone
from typing import Any, Literal
from uuid import UUID, uuid4

from sqlalchemy import Engine, exists, update
from sqlalchemy.orm import aliased
from sqlmodel import Session, col, select

from interloper_db.crypto import hash_token
from interloper_db.models import Audience, AuthorizationCode, OAuthClient, RefreshToken, Token, TokenKind
from interloper_db.session import commit, save, session_scope
from interloper_db.store.tokens import TokenStore

CLIENT_SECRET_PREFIX = "ilc_"

REFRESH_TOKEN_PREFIX = "ilr_"

AUTHORIZATION_CODE_EXPIRY = timedelta(minutes=10)


class OAuthStore:
    """Store methods for OAuth clients, authorization codes and refresh tokens."""

    def __init__(self, engine: Engine, tokens: TokenStore, *, refresh_token_expiry: timedelta) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            tokens: The token facet, which issues the access tokens a grant
                pairs with its refresh tokens.
            refresh_token_expiry: How long a refresh token stays valid; each
                rotation issues a successor valid this long again.
        """
        self._engine = engine
        self._tokens = tokens
        self._refresh_token_expiry = refresh_token_expiry

    # -- Clients ---------------------------------------------------------------

    def register_client(
        self,
        name: str,
        redirect_uris: list[str],
        *,
        registered_by: Literal["dynamic", "operator"],
        confidential: bool = False,
    ) -> tuple[OAuthClient, str | None]:
        """Register a client, generating its id and, if confidential, its secret.

        Args:
            name: Name shown to the user on the consent page and in settings.
            redirect_uris: The exact URIs a code may be sent to.
            registered_by: ``dynamic`` for RFC 7591 registration, ``operator``
                for a client created from the command line.
            confidential: Whether the client authenticates with a secret. A
                public client (``token_endpoint_auth_method`` ``none``) has
                none and relies on PKCE.

        Returns:
            ``(client, raw_secret)``; the secret is ``None`` for a public
            client, and is never stored or recoverable otherwise.
        """
        raw_secret = CLIENT_SECRET_PREFIX + secrets.token_urlsafe(36) if confidential else None
        client = OAuthClient(
            client_id=secrets.token_urlsafe(24),
            secret_hash=hash_token(raw_secret) if raw_secret else None,
            name=name,
            redirect_uris=redirect_uris,
            registered_by=registered_by,
        )
        with session_scope(self._engine) as session:
            save(session, client)
        return client, raw_secret

    def get_client(self, client_id: str) -> OAuthClient | None:
        """Look a client up by the id a request presents.

        Args:
            client_id: The client id.

        Returns:
            The client, or ``None`` when no client has that id.
        """
        with session_scope(self._engine) as session:
            return session.get(OAuthClient, client_id)

    def authorised_clients(self, user_id: UUID) -> list[OAuthClient]:
        """List the clients a user has authorised: those holding a live refresh token.

        Args:
            user_id: Profile UUID.

        Returns:
            The clients, by name.
        """
        live = select(RefreshToken.client_id).where(
            col(RefreshToken.user_id) == user_id, *self._live(datetime.now(timezone.utc))
        )
        statement = (
            select(OAuthClient)
            .where(col(OAuthClient.client_id).in_(live))
            .order_by(col(OAuthClient.name), col(OAuthClient.client_id))
        )
        with session_scope(self._engine) as session:
            return list(session.exec(statement).all())

    def revoke_client_for_user(self, user_id: UUID, client_id: str) -> None:
        """Revoke every refresh and access token a client holds for a user, and its unspent codes.

        A code approved before the revocation expires with it, so the client
        cannot exchange it for a fresh grant. Idempotent: tokens already
        revoked keep their revocation time.

        Args:
            user_id: Profile UUID.
            client_id: The client losing access.
        """
        now = datetime.now(timezone.utc)
        with session_scope(self._engine) as session:
            session.connection().execute(
                update(AuthorizationCode)
                .where(
                    col(AuthorizationCode.user_id) == user_id,
                    col(AuthorizationCode.client_id) == client_id,
                    col(AuthorizationCode.used_at).is_(None),
                    col(AuthorizationCode.expires_at) > now,
                )
                .values(expires_at=now)
            )
            session.connection().execute(
                update(RefreshToken)
                .where(
                    col(RefreshToken.user_id) == user_id,
                    col(RefreshToken.client_id) == client_id,
                    col(RefreshToken.revoked_at).is_(None),
                )
                .values(revoked_at=now)
            )
            self._revoke_access(session, user_id, client_id, now)
            commit(session)

    # -- Authorization codes ---------------------------------------------------

    def issue_code(
        self, *, client_id: str, user_id: UUID, redirect_uri: str, code_challenge: str, resource: Audience
    ) -> str:
        """Issue a single-use authorization code, valid for ten minutes.

        The code opens a new refresh-token family, whose id it carries.

        Args:
            client_id: The client the user approved.
            user_id: Profile UUID of the approving user.
            redirect_uri: The URI the code is sent to, which the exchange must repeat.
            code_challenge: The PKCE ``S256`` challenge the exchange's verifier must match.
            resource: The surface the grant is for.

        Returns:
            The raw code; only its hash is stored.
        """
        raw = secrets.token_urlsafe(32)
        code = AuthorizationCode(
            code_hash=hash_token(raw),
            family_id=uuid4(),
            client_id=client_id,
            user_id=user_id,
            redirect_uri=redirect_uri,
            code_challenge=code_challenge,
            resource=resource.value,
            expires_at=datetime.now(timezone.utc) + AUTHORIZATION_CODE_EXPIRY,
        )
        with session_scope(self._engine) as session:
            save(session, code)
        return raw

    def consume_code(self, raw: str) -> AuthorizationCode | None:
        """Spend an authorization code, once.

        The caller checks the returned code against the request: its client,
        redirect URI and PKCE verifier.

        Args:
            raw: The code as the client presents it.

        Returns:
            The code, now marked used, or ``None`` when it is unknown, expired
            or already spent. Presenting a spent code within its lifetime also
            revokes the family it opened; past it, the code was exchanged long
            enough ago that its replay says nothing new, and revokes nothing.
        """
        now = datetime.now(timezone.utc)
        with session_scope(self._engine) as session:
            code = session.exec(
                select(AuthorizationCode).where(col(AuthorizationCode.code_hash) == hash_token(raw)).with_for_update()
            ).first()
            if code is None or code.expires_at <= now:
                return None
            if code.used_at is not None:
                self._revoke_family(session, code.family_id, code.user_id, code.client_id, now)
                commit(session)
                return None
            code.used_at = now
            save(session, code)
            return code

    # -- Refresh tokens --------------------------------------------------------

    def issue_refresh(self, user_id: UUID, client_id: str, resource: Audience, family_id: UUID | None = None) -> str:
        """Issue a refresh token, valid for the configured refresh lifetime from now.

        Args:
            user_id: Profile UUID of the user the client acts for.
            client_id: The client holding the token.
            resource: The surface the grant is for.
            family_id: The family the token joins: the consumed code's on a
                code exchange, the rotated token's on a refresh. ``None``
                starts a new family.

        Returns:
            The raw token; only its hash is stored.
        """
        raw = REFRESH_TOKEN_PREFIX + secrets.token_urlsafe(36)
        token = RefreshToken(
            token_hash=hash_token(raw),
            family_id=family_id or uuid4(),
            client_id=client_id,
            user_id=user_id,
            resource=resource.value,
            expires_at=datetime.now(timezone.utc) + self._refresh_token_expiry,
        )
        with session_scope(self._engine) as session:
            save(session, token)
        return raw

    def issue_tokens(
        self, user_id: UUID, client_id: str, resource: Audience, family_id: UUID
    ) -> tuple[str, str] | None:
        """Issue a grant's access token and the refresh token that joins its family.

        The family is checked again once the pair is written: a replay that
        revoked it while the pair was being issued would not have reached
        rows it could not see yet, so the pair is revoked here instead.

        Args:
            user_id: Profile UUID of the user the client acts for.
            client_id: The client holding the tokens.
            resource: The surface the grant is for.
            family_id: The family the refresh token joins: the consumed code's
                on a code exchange, the rotated token's on a refresh.

        Returns:
            ``(access_token, refresh_token)``, raw; only their hashes are
            stored. ``None`` when the family was revoked, leaving both revoked.
        """
        with session_scope(self._engine) as session:
            access = self._tokens.create_access(user_id, client_id, resource)
            refresh = self.issue_refresh(user_id, client_id, resource, family_id)
            if not self._family_revoked(session, family_id):
                commit(session)
                return access, refresh
            now = datetime.now(timezone.utc)
            session.connection().execute(
                update(Token).where(col(Token.token_hash) == hash_token(access)).values(revoked_at=now)
            )
            session.connection().execute(
                update(RefreshToken).where(col(RefreshToken.token_hash) == hash_token(refresh)).values(revoked_at=now)
            )
            commit(session)
            return None

    def rotate_refresh(self, raw: str, client_id: str) -> RefreshToken | None:
        """Retire a refresh token presented by its client, once.

        The caller then issues its successor with ``issue_refresh`` in the
        returned token's family.

        Args:
            raw: The refresh token as the client presents it.
            client_id: The authenticated client presenting it.

        Returns:
            The token, now marked rotated, or ``None`` when it is unknown,
            another client's, expired or revoked. Presenting a token already
            rotated also revokes its family.
        """
        now = datetime.now(timezone.utc)
        with session_scope(self._engine) as session:
            token = session.exec(
                select(RefreshToken).where(col(RefreshToken.token_hash) == hash_token(raw)).with_for_update()
            ).first()
            if token is None or token.client_id != client_id:
                return None
            if token.rotated_at is not None:
                self._revoke_family(session, token.family_id, token.user_id, token.client_id, now)
                commit(session)
                return None
            if (
                token.revoked_at is not None
                or token.expires_at <= now
                or self._family_revoked(session, token.family_id)
            ):
                return None
            token.rotated_at = now
            save(session, token)
            return token

    def find_refresh(self, raw: str) -> RefreshToken | None:
        """Look a refresh token up by its raw value, whatever its state.

        Args:
            raw: The refresh token as presented.

        Returns:
            The token, or ``None`` when no refresh token has that value.
        """
        with session_scope(self._engine) as session:
            return session.get(RefreshToken, hash_token(raw))

    def revoke_family(self, token: RefreshToken) -> None:
        """Revoke a refresh token's family and its client's access tokens for its user.

        Idempotent: tokens already revoked keep their revocation time.

        Args:
            token: A token of the family, typically one presented for revocation.
        """
        with session_scope(self._engine) as session:
            self._revoke_family(session, token.family_id, token.user_id, token.client_id, datetime.now(timezone.utc))
            commit(session)

    # -- Internals -------------------------------------------------------------

    def _live(self, now: datetime) -> tuple[Any, ...]:
        """The conditions a refresh token meets while it can still be rotated.

        Args:
            now: The instant to judge expiry at.

        Returns:
            Filter clauses over ``RefreshToken``: not rotated, not revoked,
            not expired, and its family not revoked.
        """
        revoked_sibling = aliased(RefreshToken)
        return (
            col(RefreshToken.rotated_at).is_(None),
            col(RefreshToken.revoked_at).is_(None),
            col(RefreshToken.expires_at) > now,
            ~exists().where(
                col(revoked_sibling.family_id) == col(RefreshToken.family_id),
                col(revoked_sibling.revoked_at).is_not(None),
            ),
        )

    def _family_revoked(self, session: Session, family_id: UUID) -> bool:
        """Whether any token of a refresh-token family has been revoked.

        Args:
            session: The session to read in.
            family_id: The family.

        Returns:
            ``True`` when the family was revoked, even if some of its tokens
            were saved into it afterwards.
        """
        statement = select(RefreshToken.token_hash).where(
            col(RefreshToken.family_id) == family_id, col(RefreshToken.revoked_at).is_not(None)
        )
        return session.exec(statement.limit(1)).first() is not None

    def _revoke_family(self, session: Session, family_id: UUID, user_id: UUID, client_id: str, now: datetime) -> None:
        """Revoke a family's refresh tokens and its client's access tokens for its user, once.

        A family already revoked is left alone: its access tokens were revoked
        with it, and those the client holds now belong to a newer grant.

        Args:
            session: The session to write in; the caller commits.
            family_id: The family to revoke.
            user_id: The family's user.
            client_id: The family's client.
            now: The revocation time to stamp.
        """
        if self._family_revoked(session, family_id):
            return
        session.connection().execute(
            update(RefreshToken)
            .where(col(RefreshToken.family_id) == family_id, col(RefreshToken.revoked_at).is_(None))
            .values(revoked_at=now)
        )
        self._revoke_access(session, user_id, client_id, now)

    def _revoke_access(self, session: Session, user_id: UUID, client_id: str, now: datetime) -> None:
        """Revoke the live access tokens a client holds for a user.

        Args:
            session: The session to write in; the caller commits.
            user_id: Profile UUID.
            client_id: The client.
            now: The revocation time to stamp.
        """
        session.connection().execute(
            update(Token)
            .where(
                col(Token.user_id) == user_id,
                col(Token.client_id) == client_id,
                col(Token.kind) == TokenKind.ACCESS,
                col(Token.revoked_at).is_(None),
            )
            .values(revoked_at=now)
        )
