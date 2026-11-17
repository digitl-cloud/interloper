"""A new account proposed as another way to sign in to the profile holding its address."""

from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from dataclasses import asdict, dataclass
from datetime import timedelta
from typing import Any, ClassVar, Self
from uuid import UUID

LINK_EXPIRY = timedelta(hours=1)


@dataclass(frozen=True)
class LinkProposal:
    """An account a login proved, proposed as another way to sign in to the profile holding its address.

    The person proves the profile is theirs before the account joins it. The
    proposal waits for that proof in the login session, for signing in the
    way the profile already does, or in a link emailed to the profile's
    address; the session is signed and the link encrypted, under keys derived
    from the instance's secret, so the proposal is never stored.

    Attributes:
        issuer: The new account's issuer.
        subject: The new account's subject.
        provider: The name of the provider that proved it.
        profile_id: The profile holding its address.
        email: That profile's address, where the link is emailed.
        destination: Where the login that proposed it asked to land.
    """

    issuer: str
    subject: str
    provider: str
    profile_id: UUID
    email: str
    destination: str

    _KEY: ClassVar[str] = "link_proposal"

    @classmethod
    def from_mapping(cls, stored: object) -> Self | None:
        """Read a proposal from its plain-values form.

        Args:
            stored: What a session or a sealed link carried.

        Returns:
            The proposal, or ``None`` when the values are no proposal.
        """
        if not isinstance(stored, Mapping):
            return None
        try:
            return cls(
                issuer=str(stored["issuer"]),
                subject=str(stored["subject"]),
                provider=str(stored["provider"]),
                profile_id=UUID(str(stored["profile_id"])),
                email=str(stored["email"]),
                destination=str(stored["destination"]),
            )
        except (KeyError, ValueError):
            return None

    @classmethod
    def from_session(cls, session: MutableMapping[str, Any]) -> Self | None:
        """Read the proposal a login session holds.

        Args:
            session: The login session.

        Returns:
            The proposal, or ``None`` when the session holds none.
        """
        return cls.from_mapping(session.get(cls._KEY))

    @classmethod
    def take(cls, session: MutableMapping[str, Any]) -> Self | None:
        """Read the proposal a login session holds, and drop it from the session.

        Args:
            session: The login session.

        Returns:
            The proposal, or ``None`` when the session held none.
        """
        proposal = cls.from_session(session)
        cls.discard(session)
        return proposal

    @classmethod
    def discard(cls, session: MutableMapping[str, Any]) -> None:
        """Drop any proposal a login session holds.

        Args:
            session: The login session.
        """
        session.pop(cls._KEY, None)

    def as_mapping(self) -> dict[str, str]:
        """Write the proposal in its plain-values form.

        Returns:
            The proposal's fields as strings.
        """
        return {**asdict(self), "profile_id": str(self.profile_id)}

    def save(self, session: MutableMapping[str, Any]) -> None:
        """Keep the proposal in a login session, replacing any other.

        Args:
            session: The login session.
        """
        session[self._KEY] = self.as_mapping()
