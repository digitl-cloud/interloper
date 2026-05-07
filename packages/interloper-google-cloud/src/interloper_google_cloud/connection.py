"""Google Cloud connection resource for service account credentials."""

from __future__ import annotations

import json

from interloper.connection import Connection, connection
from interloper.resource.fields import JsonField
from pydantic import field_validator
from pydantic_settings import SettingsConfigDict


@connection(
    key="google_cloud_connection",
    name="Google Cloud",
    icon="devicon:googlecloud",
    tags=["Cloud"],
)
class GoogleCloudConnection(Connection):
    """Connection resource holding Google Cloud credentials."""

    model_config = SettingsConfigDict(env_prefix="google_cloud_")

    service_account_key: str = JsonField()

    @field_validator("service_account_key", mode="before")
    @classmethod
    def _serialize_key(cls, v: object) -> object:
        if isinstance(v, dict):
            return json.dumps(v)
        return v
