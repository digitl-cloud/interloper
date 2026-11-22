"""Token persistence: create, resolve, list, revoke.

Tokens are opaque bearer secrets whose SHA-256 hash is the only thing
stored. A token stands for its user and carries no organisation: resolving
one yields the holder, and resolving it in an organisation also reads the
holder's live role there, so a role change or removal applies at once. A
login session is a token too, the one the app's cookie carries, and so is
an OAuth access token, which a client holds for the surface it was issued for.
OAuth refresh tokens are rows here as well, but the OAuth facet issues,
rotates and purges them, and no surface resolves one.
"""

from __future__ import annotations

import secrets
from datetime import UTC, datetime, timedelta
from uuid import UUID

from sqlalchemy import Engine, delete, update
from sqlmodel import Session, col, select

from interloper_db.crypto import hash_token
from interloper_db.errors import NotFoundError
from interloper_db.models import Audience, Organisation, Profile, Token, TokenKind, UserOrganisation
from interloper_db.session import commit, save, session_scope
from interloper_db.store.page import Page, PageQuery

TOKEN_PREFIX = "ilp_"

ACCESS_TOKEN_PREFIX = "ila_"

TOKEN_PREFIX_LEN = 12

# last_used_at is informational; throttling the bump keeps hot MCP sessions
# from turning every tool call into a write.
LAST_USED_THROTTLE_SECONDS = 60


class TokenStore:
    """Store methods for tokens."""

    def __init__(self, engine: Engine, *, session_expiry: timedelta, access_token_expiry: timedelta) -> None:
        """Bind the facet to what it works through.

        Args:
            engine: Engine the facet opens its sessions on.
            session_expiry: How long a login session stays valid.
            access_token_expiry: How long an OAuth access token stays valid.
        """
        self._engine = engine
        self._session_expiry = session_expiry
        self._access_token_expiry = access_token_expiry

    def create_session(self, user_id: UUID) -> str:
        """Open a login session and return its raw secret, the cookie's value.

        A session is never listed or shown, so it keeps no display prefix:
        its ``token_prefix`` is empty, as on the sessions migration 017 moved.

        Args:
            user_id: Profile UUID of the user logging in.

        Returns:
            The raw secret; only its hash is stored.
        """
        raw = secrets.token_urlsafe(48)
        with session_scope(self._engine) as session:
            save(
                session,
                Token(
                    user_id=user_id,
                    kind=TokenKind.SESSION,
                    token_prefix="",
                    token_hash=hash_token(raw),
                    expires_at=datetime.now(UTC) + self._session_expiry,
                ),
            )
        return raw

    def revoke_sessions(self, user_id: UUID, *, keep: str | None = None) -> None:
        """Revoke every live login session of a user (logout everywhere).

        Args:
            user_id: Profile UUID.
            keep: The raw secret of one session to leave live, the caller's
                own; ``None`` revokes them all.
        """
        statement = update(Token).where(
            col(Token.user_id) == user_id,
            col(Token.kind) == TokenKind.SESSION,
            col(Token.revoked_at).is_(None),
        )
        if keep is not None:
            statement = statement.where(col(Token.token_hash) != hash_token(keep))
        with session_scope(self._engine) as session:
            session.connection().execute(statement.values(revoked_at=datetime.now(UTC)))
            commit(session)

    def create_access(self, user_id: UUID, client_id: str, resource: Audience) -> str:
        """Issue an OAuth access token a client holds for a user on one surface.

        Like a session, an access token is never listed, so it keeps no
        display prefix.

        Args:
            user_id: Profile UUID of the user the client acts for.
            client_id: The OAuth client holding the token.
            resource: The surface the token is issued for; it resolves there only.

        Returns:
            The raw token; only its hash is stored.
        """
        raw = ACCESS_TOKEN_PREFIX + secrets.token_urlsafe(36)
        with session_scope(self._engine) as session:
            save(
                session,
                Token(
                    user_id=user_id,
                    kind=TokenKind.ACCESS,
                    client_id=client_id,
                    resource=resource.value,
                    token_prefix="",
                    token_hash=hash_token(raw),
                    expires_at=datetime.now(UTC) + self._access_token_expiry,
                ),
            )
        return raw

    def find_access(self, raw: str) -> Token | None:
        """Look an OAuth access token up by its raw value, whatever its state or surface.

        For the authorization server's revocation endpoint, which a client
        calls with the token alone.

        Args:
            raw: The access token as presented.

        Returns:
            The token, or ``None`` when no access token has that value; a
            session or personal token never matches.
        """
        statement = select(Token).where(Token.token_hash == hash_token(raw), Token.kind == TokenKind.ACCESS)
        with session_scope(self._engine) as session:
            return session.exec(statement).first()

    def create_personal(self, user_id: UUID, name: str, expires_at: datetime | None = None) -> tuple[Token, str]:
        """Create a personal access token.

        Args:
            user_id: Profile UUID of the token holder.
            name: User-facing label (e.g. "Claude Code laptop").
            expires_at: Optional expiry; ``None`` means the token never expires.

        Returns:
            ``(row, raw_token)`` — the raw token is never stored and cannot be
            recovered later.
        """
        raw = TOKEN_PREFIX + secrets.token_urlsafe(36)

        with session_scope(self._engine) as session:
            db_token = Token(
                user_id=user_id,
                kind=TokenKind.PERSONAL,
                name=name,
                token_prefix=raw[:TOKEN_PREFIX_LEN],
                token_hash=hash_token(raw),
                expires_at=expires_at,
            )
            save(session, db_token)
            return db_token, raw

    def resolve(self, raw: str, audience: Audience) -> tuple[Profile, Token] | None:
        """Resolve a raw secret presented to a surface to its holder and row, in one statement.

        Args:
            raw: The raw secret as presented by the client.
            audience: The surface it was presented to; a token of a kind the
                surface does not accept, or an access token issued for
                another surface, does not resolve.

        Returns:
            ``(Profile, Token)`` when the token is valid for the surface, else
            ``None``: on no match, a kind or resource the surface refuses,
            revocation, expiry, or a deleted profile.
        """
        statement = (
            select(Token, Profile)
            .join(Profile, col(Profile.id) == col(Token.user_id))
            .where(col(Token.token_hash) == hash_token(raw))
        )
        with session_scope(self._engine) as session:
            row = session.exec(statement).first()
            if row is None or not self._admit(session, row[0], audience):
                return None
            db_token, db_profile = row
            return db_profile, db_token

    def resolve_in(
        self, raw: str, audience: Audience, org_id: UUID
    ) -> tuple[Profile, Token, Organisation | None, str | None] | None:
        """Resolve a raw secret presented to a surface, and its holder's standing in an organisation, in one statement.

        A request acting in an organisation authenticates and authorises in
        one round trip: the organisation and the holder's membership are read
        with the token, so the role is as live as the token's validity.

        Args:
            raw: The raw secret as presented by the client.
            audience: The surface it was presented to, refused as by :meth:`resolve`.
            org_id: The organisation the holder acts in.

        Returns:
            ``(Profile, Token, organisation, role)`` when the token is valid
            for the surface, else ``None`` as for :meth:`resolve`. The
            organisation is ``None`` when no live organisation carries
            ``org_id``; the role is :meth:`Profile.acting_role`'s there,
            ``None`` when the holder cannot act in it.
        """
        statement = (
            select(Token, Profile, Organisation, UserOrganisation.role)
            .join(Profile, col(Profile.id) == col(Token.user_id))
            .outerjoin(Organisation, (col(Organisation.id) == org_id) & col(Organisation.deleted_at).is_(None))
            .outerjoin(
                UserOrganisation,
                (col(UserOrganisation.user_id) == col(Token.user_id))
                & (col(UserOrganisation.organisation_id) == col(Organisation.id)),
            )
            .where(col(Token.token_hash) == hash_token(raw))
        )
        with session_scope(self._engine) as session:
            row = session.exec(statement).first()
            if row is None or not self._admit(session, row[0], audience):
                return None
            db_token, db_profile, organisation, membership = row
            role = db_profile.acting_role(membership) if organisation is not None else None
            return db_profile, db_token, organisation, role

    def list(self, user_id: UUID, query: PageQuery) -> Page[Token]:
        """List a user's personal access tokens, newest first.

        Args:
            user_id: Profile UUID of the holder.
            query: The window to read.

        Returns:
            The page of tokens. Rows carry only the display prefix and the
            hash — exposing neither raw secrets nor anything recoverable.
        """
        statement = (
            select(Token)
            .where(Token.user_id == user_id, Token.kind == TokenKind.PERSONAL)
            .order_by(col(Token.created_at).desc(), col(Token.id))
        )
        with session_scope(self._engine) as session:
            return Page.read(session, statement, query)

    def get(self, token_id: UUID) -> Token:
        """Get a token row by ID.

        Args:
            token_id: Token UUID.

        Returns:
            The token row.

        Raises:
            NotFoundError: If no token carries that id.
        """
        with session_scope(self._engine) as session:
            db_token = session.get(Token, token_id)
            if not db_token:
                raise NotFoundError(f"Token {token_id} not found")
            return db_token

    def revoke(self, token_id: UUID) -> Token:
        """Revoke a token (soft delete: the row stays for audit until :meth:`purge` deletes it).

        Args:
            token_id: Token UUID.

        Returns:
            The revoked row. Revoking an already-revoked token is a no-op.
        """
        with session_scope(self._engine) as session:
            db_token = self.get(token_id)
            if db_token.revoked_at is None:
                db_token.revoked_at = datetime.now(UTC)
                save(session, db_token)
            return db_token

    def purge(self, *, ended_for: timedelta) -> int:
        """Delete the tokens that expired or were revoked more than ``ended_for`` ago.

        Sessions, personal tokens and OAuth access tokens go the same way. A
        token that never expires and was never revoked is kept. Refresh
        tokens are left to the OAuth facet's purge, which deletes a family
        whole once it ended, so a replay of any of its tokens is caught until then.

        Args:
            ended_for: How long a token is kept once it expired or was revoked.

        Returns:
            The number of tokens deleted.
        """
        cutoff = datetime.now(UTC) - ended_for
        statement = delete(Token).where(
            col(Token.kind) != TokenKind.REFRESH, (col(Token.expires_at) < cutoff) | (col(Token.revoked_at) < cutoff)
        )
        with session_scope(self._engine) as session:
            deleted = session.connection().execute(statement).rowcount
            commit(session)
        return deleted

    def _admit(self, session: Session, db_token: Token, audience: Audience) -> bool:
        """Decide whether a surface accepts a token now, stamping its use when it does.

        The stamp commits with the resolving call, as any store write does
        when its call ends. In a shared session that ends the transaction the
        resolving read began, which holds nothing but reads, and the next call
        begins a fresh one.

        Args:
            session: The session the token was read in.
            db_token: The token row.
            audience: The surface the token was presented to.

        Returns:
            True when the surface accepts the token's kind and, for an access
            token, its resource, and the token is neither revoked nor expired.
        """
        now = datetime.now(UTC)
        if db_token.kind not in audience.kinds:
            return False
        if db_token.kind == TokenKind.ACCESS and db_token.resource != audience.value:
            return False
        if db_token.revoked_at is not None:
            return False
        if db_token.expires_at is not None and db_token.expires_at < now:
            return False
        last_used = db_token.last_used_at
        if last_used is None or last_used < now - timedelta(seconds=LAST_USED_THROTTLE_SECONDS):
            db_token.last_used_at = now
            commit(session)
        return True
