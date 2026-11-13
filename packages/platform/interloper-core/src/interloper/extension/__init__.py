"""Extension manifests: what each installed package declares, read by the registries."""

from interloper.extension.base import MANIFEST_FILE, MANIFESTS, Manifests
from interloper.extension.entry import Extension, ExtensionEntry, ExtensionFailure

__all__ = [
    "MANIFESTS",
    "MANIFEST_FILE",
    "Extension",
    "ExtensionEntry",
    "ExtensionFailure",
    "Manifests",
]
