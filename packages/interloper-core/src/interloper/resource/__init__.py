"""Resources: injectable dependencies, their typed slots, and their field helpers."""

from interloper.resource.base import Resource, ResourceDefinition
from interloper.resource.fields import (
    FetchField,
    InputField,
    JsonField,
    SecretField,
    SelectField,
    TextField,
    fetch_field_provider,
    is_fetch_field_provider,
)

__all__ = [
    "FetchField",
    "InputField",
    "JsonField",
    "Resource",
    "ResourceDefinition",
    "SecretField",
    "SelectField",
    "TextField",
    "fetch_field_provider",
    "is_fetch_field_provider",
]
