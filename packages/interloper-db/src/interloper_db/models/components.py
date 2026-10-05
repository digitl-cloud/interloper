"""Component instances and the typed relations between them."""

import json
from collections.abc import Callable
from datetime import datetime
from typing import Any, ClassVar, Optional
from uuid import UUID, uuid4

import interloper as il
from interloper.errors import ConfigError, HydrationError, format_exception
from interloper.utils.time import assume_utc
from sqlalchemy import CheckConstraint, ForeignKey, ForeignKeyConstraint, Index, UniqueConstraint
from sqlalchemy.orm.base import NO_VALUE, instance_state
from sqlmodel import Column, LargeBinary, Relationship, SQLModel, text
from sqlmodel import Field as SQLField

from interloper_db.models.columns import PortableJSON, timestamp_column


class Component(SQLModel, table=True):
    """A persisted component instance of any kind.

    ``kind``/``key`` mirror the framework class identity; ``parent_id`` models
    ownership (asset → source, cascading on delete). ``config`` holds the
    spec, ``state`` holds operator-written runtime state, ``data`` holds the
    encrypted payload of secret-bearing kinds.

    Two persistence details are load-bearing. ``id`` carries a Python-side
    default on top of ``gen_random_uuid()`` so the store can wire relations to
    a component before flush, and so inserts work on the SQLite test
    databases. And ``children`` uses ``passive_deletes="all"`` rather than
    ``True``: deletion is owned by the DB (``parent_id`` is ``ON DELETE
    CASCADE``), and ``True`` still nulls ``parent_id`` on children that happen
    to be loaded in the deleting session, detaching them from the cascade and
    leaving orphaned asset rows.
    """

    __tablename__: ClassVar[str] = "components"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        UniqueConstraint("id", "org_id", "kind", name="uq_components_id_org_kind"),
        Index("ix_components_org_id_kind", "org_id", "kind"),
        CheckConstraint("parent_id IS NULL OR kind = 'asset'", name="ck_components_parent_kind"),
        # Snapshot of the sensitive kinds (see interloper.KINDS) — a new
        # sensitive kind needs this CHECK widened in a migration.
        CheckConstraint(
            "data IS NULL OR kind IN ('connection', 'config', 'resource')",
            name="ck_components_data_kind",
        ),
    )

    id: UUID = SQLField(
        default_factory=uuid4,
        primary_key=True,
        sa_column_kwargs={"server_default": text("gen_random_uuid()")},
    )
    org_id: UUID
    kind: str
    key: str = SQLField(index=True)
    name: str | None = None
    parent_id: UUID | None = SQLField(
        default=None,
        sa_column=Column(ForeignKey("components.id", ondelete="CASCADE"), index=True, nullable=True),
    )
    config: dict[str, Any] | None = SQLField(default=None, sa_column=Column(PortableJSON))
    state: dict[str, Any] | None = SQLField(default=None, sa_column=Column(PortableJSON))
    data: bytes | None = SQLField(default=None, sa_column=Column(LargeBinary))
    encrypted: bool = False
    created_at: datetime | None = timestamp_column()
    updated_at: datetime | None = timestamp_column(onupdate=text("CURRENT_TIMESTAMP"))

    # Spelled Optional[...] rather than "Component" | None: SQLModel cannot
    # resolve the union string form at mapper-configuration time.
    parent: Optional["Component"] = Relationship(
        back_populates="children",
        sa_relationship_kwargs={"remote_side": "Component.id"},
    )
    children: list["Component"] = Relationship(
        back_populates="parent",
        sa_relationship_kwargs={"passive_deletes": "all"},
    )
    out_relations: list["ComponentRelation"] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "Component.id == foreign(ComponentRelation.src_id)",
            "viewonly": True,
        },
    )
    in_relations: list["ComponentRelation"] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "Component.id == foreign(ComponentRelation.dst_id)",
            "viewonly": True,
        },
    )

    @property
    def qualified_key(self) -> str:
        """The row's catalog key, qualified by its owning source's key when it has one.

        The framework's own form (``Component.qualified_key``), and the one
        ``Catalog.get`` resolves: an owned asset is declared inside its
        source, so its bare key names nothing on its own. Reads the parent
        row the store's listings load alongside, and only reaches for an
        unloaded one when the row has a parent to load.

        Returns:
            ``source.asset`` for an owned asset, the bare key otherwise.
        """
        parent = instance_state(self).attrs.parent.loaded_value
        if parent is NO_VALUE:
            parent = self.parent if self.parent_id is not None else None
        return self.key if parent is None else f"{parent.key}.{self.key}"

    def write_config(
        self, config: dict[str, Any] | None, *, encrypt: Callable[[bytes], bytes] | None, encrypted: bool | None
    ) -> None:
        """Store a config payload on the row, encrypting a sensitive kind's.

        A sensitive kind's payload is serialised into ``data``, encrypted
        unless *encrypted* opts out, and ``config`` is left empty; any other
        kind's is stored as ``config``.

        Args:
            config: The payload to store; ``None`` stores an empty one.
            encrypt: Callable encrypting a serialised payload, or ``None``
                when no encryption key is configured.
            encrypted: Sensitive kinds only: ``True``/``None`` encrypt,
                ``False`` stores the payload in plaintext.

        Raises:
            ConfigError: If a sensitive payload must be encrypted but no
                encryption key is configured, so secrets are never silently
                written in plaintext.
        """
        if not il.KINDS[self.kind].sensitive:
            self.config = config
            return
        should_encrypt = True if encrypted is None else encrypted
        raw = json.dumps(config or {}).encode()
        if should_encrypt:
            if encrypt is None:
                raise ConfigError(
                    "Refusing to store a resource without encryption at rest: "
                    "INTERLOPER_ENCRYPTION_KEY is not configured. Set it, or pass "
                    "encrypted=false to store this resource in plaintext."
                )
            raw = encrypt(raw)
        self.data, self.encrypted, self.config = raw, should_encrypt, None

    def read_config(self, decrypt: Callable[[bytes], bytes] | None) -> dict[str, Any]:
        """The row's config payload, decrypting a sensitive kind's.

        Args:
            decrypt: Callable decrypting a stored payload, or ``None`` when no
                encryption key is configured.

        Returns:
            The decoded payload; empty when the row carries none.

        Raises:
            HydrationError: If the payload is encrypted but no key is
                configured, or does not decrypt under the configured one.
        """
        if not il.KINDS[self.kind].sensitive:
            return dict(self.config or {})
        if self.data is None:
            return {}
        raw = self.data
        if self.encrypted:
            if decrypt is None:
                raise HydrationError(
                    f"Component {self.id} is encrypted but INTERLOPER_ENCRYPTION_KEY is not configured; cannot decrypt"
                )
            try:
                raw = decrypt(raw)
            except Exception as e:
                raise HydrationError(
                    f"Failed to decrypt component {self.id}; the configured INTERLOPER_ENCRYPTION_KEY may be "
                    f"wrong or the data was not encrypted with it: {format_exception(e)}"
                ) from e
        return json.loads(raw)

    def stamp_state(self, **fields: Any) -> None:
        """Merge machine-owned state fields onto a component row (spec untouched).

        Datetimes are written in the canonical timezone-aware ISO form (so
        lexicographic comparison in SQL stays chronological); the merged payload
        is validated against the kind's ``state_model`` — shape only, stored
        strings are never rewritten. The caller owns the session and commit.

        Args:
            self: The row to merge the state onto.
            **fields: State fields to set, merged over the existing payload.
                Datetime values are stored as timezone-aware ISO strings.
        """
        import datetime as dt

        state = dict(self.state or {})
        for key, value in fields.items():
            state[key] = value.isoformat() if isinstance(value, dt.datetime) else value
        model = il.KINDS[self.kind].state_model
        if model is not None:
            model.model_validate(state)
        self.state = state

    def state_datetime(self, key: str) -> datetime | None:
        """Read a timestamp out of the row's machine-owned state.

        Args:
            key: The state key holding an ISO-8601 timestamp.

        Returns:
            The timestamp as an aware datetime, a naive stored value read as
            UTC, or ``None`` when the key is absent or empty.
        """
        value = (self.state or {}).get(key)
        if not value:
            return None
        return assume_utc(datetime.fromisoformat(value))

    def state_text(self, key: str) -> str | None:
        """Read a text value out of the row's machine-owned state.

        Args:
            key: The state key holding the text.

        Returns:
            The stored text, or ``None`` when the key is absent or empty.
        """
        value = (self.state or {}).get(key)
        return str(value) if value else None

    def run_billable(self) -> bool:
        """Whether runs targeting this component count against the run quota.

        The kind must declare a workload (its anchor subclasses ``Workload``),
        whose ``billable`` decides.

        Returns:
            The billability the kind's workload declares.

        Raises:
            ConfigError: If the kind's anchor declares no workload, so the
                component cannot be run at all.
        """
        anchor = il.KINDS[self.kind]
        if not issubclass(anchor, il.Workload):
            raise ConfigError(f"Components of kind '{self.kind}' cannot be run")
        return anchor.billable

    @property
    def retry_policy(self) -> il.RetryPolicy | None:
        """The run-level retry policy a job's config declares.

        A job's declared policy governs its runs, and nothing else does: a
        source's or an asset's own ``retry`` is an operation budget, and no
        instance-wide default exists, so a row of any other kind, or a job
        declaring nothing, is attempted once.

        Returns:
            The policy, or ``None`` when the row declares none.
        """
        declared = (self.config or {}).get("retry") if self.kind == "job" else None
        return il.RetryPolicy.model_validate(declared) if declared else None

    @property
    def identity(self) -> il.ComponentIdentity:
        """What the row is, for relation matching: its owning source's key and its own.

        Returns:
            The identity, the framework's own for a live component.
        """
        return il.ComponentIdentity.resolve(self.qualified_key, own_source_key=None)

    @property
    def enabled(self) -> bool:
        """Whether the row's config leaves it enabled, which is the default.

        Reads the plain ``config`` column, never the encrypted ``data``
        payload, so it holds for every kind without decrypting.

        Returns:
            ``False`` only when the config sets ``enabled`` to ``False``; an
            absent or null flag reads as enabled.
        """
        return (self.config or {}).get("enabled") is not False


class ComponentRelation(SQLModel, table=True):
    """A named, directed relation between two components.

    ``name`` is the relation name as declared on the owning class: the field
    a ``Relation`` is bound to. Whether a given name is single-valued or
    many-valued is a class rule the store enforces, not a schema constraint,
    so no uniqueness is declared here. ``dst`` reads the target row the edge
    points at, so a consumer can name what a component is bound to without a
    second lookup.
    """

    __tablename__: ClassVar[str] = "component_relations"
    __table_args__: ClassVar[tuple[Any, ...]] = (
        ForeignKeyConstraint(
            ["src_id", "org_id", "src_kind"],
            ["components.id", "components.org_id", "components.kind"],
            ondelete="CASCADE",
            name="fk_component_relations_src",
        ),
        ForeignKeyConstraint(
            ["dst_id", "org_id", "dst_kind"],
            ["components.id", "components.org_id", "components.kind"],
            ondelete="CASCADE",
            name="fk_component_relations_dst",
        ),
        Index("ix_component_relations_org_id_name", "org_id", "name"),
        Index("ix_component_relations_dst_id_name", "dst_id", "name"),
    )

    src_id: UUID = SQLField(primary_key=True)
    name: str = SQLField(primary_key=True)
    dst_id: UUID = SQLField(primary_key=True)
    org_id: UUID
    src_kind: str
    dst_kind: str

    dst: Optional["Component"] = Relationship(
        sa_relationship_kwargs={
            "primaryjoin": "foreign(ComponentRelation.dst_id) == Component.id",
            "viewonly": True,
        },
    )
