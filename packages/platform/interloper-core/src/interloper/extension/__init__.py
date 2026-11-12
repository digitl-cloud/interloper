"""Extension manifests: what each installed package declares, read by the registries."""

from interloper.extension.base import MANIFEST_FILE, MANIFESTS, Manifests
from interloper.extension.entry import ExtensionEntry, ExtensionFailure

__all__ = [
    "MANIFESTS",
    "MANIFEST_FILE",
    "ExtensionEntry",
    "ExtensionFailure",
    "Manifests",
]
