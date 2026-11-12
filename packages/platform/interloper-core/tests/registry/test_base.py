"""Tests for ``interloper.registry.base``."""

import threading
import time
from unittest import mock

import pytest

from interloper.extension import ExtensionFailure, Manifests
from interloper.registry import Registry


def _entry(name: str, loaded: object, package: str = "acme") -> mock.Mock:
    """Build a stub manifest entry that loads to *loaded*.

    Args:
        name: The entry's name.
        loaded: The object the entry loads to.
        package: The package declaring the entry.

    Returns:
        The stubbed entry.
    """
    entry = mock.Mock()
    entry.name = name
    entry.package = package
    entry.section = "things"
    entry.path = f"{package}.things:{name}"
    entry.load.return_value = loaded
    return entry


def _declaring(*entries: mock.Mock) -> mock.Mock:
    """Build manifests declaring *entries* under every section.

    Args:
        *entries: The entries every section holds.

    Returns:
        The stubbed manifests, to patch in for the registry module's.
    """
    manifests = mock.Mock(spec=Manifests)
    manifests.entries.return_value = list(entries)
    return manifests


class TestRegister:
    def test_register_and_get(self):
        registry: Registry[str] = Registry()
        registry.register("alpha", "A")
        assert registry.get("alpha") == "A"
        assert registry.get("missing") is None

    def test_first_wins(self):
        registry: Registry[str] = Registry()
        registry.register("alpha", "first")
        registry.register("alpha", "second")
        assert registry.get("alpha") == "first"

    def test_contains(self):
        registry: Registry[str] = Registry()
        registry.register("alpha", "A")
        assert "alpha" in registry
        assert "missing" not in registry


class TestViews:
    def test_sorted_views(self):
        registry: Registry[int] = Registry()
        registry.register("beta", 2)
        registry.register("alpha", 1)
        assert registry.keys() == ("alpha", "beta")
        assert registry.values() == (1, 2)
        assert registry.items() == (("alpha", 1), ("beta", 2))


class TestIteration:
    def test_iterates_names_in_keys_order(self):
        registry: Registry[int] = Registry()
        registry.register("beta", 2)
        registry.register("alpha", 1)
        assert list(registry) == ["alpha", "beta"]
        assert list(registry) == list(registry.keys())

    def test_iteration_does_not_fall_back_to_the_sequence_protocol(self):
        """Without ``__iter__``, ``__getitem__`` alone makes Python try ``registry[0]``."""
        registry: Registry[str] = Registry()
        registry.register("alpha", "A")
        assert [name for name in registry] == ["alpha"]

    def test_empty_registry_iterates_empty(self):
        registry: Registry[str] = Registry()
        assert list(registry) == []

    def test_len_counts_entries(self):
        registry: Registry[int] = Registry()
        assert len(registry) == 0
        registry.register("alpha", 1)
        registry.register("beta", 2)
        assert len(registry) == 2

    def test_iteration_loads_the_section(self):
        registry: Registry[str] = Registry("things")
        with mock.patch("interloper.registry.base.MANIFESTS", _declaring(_entry("alpha", "A"))):
            assert list(registry) == ["alpha"]

    def test_len_loads_the_section(self):
        registry: Registry[str] = Registry("things")
        with mock.patch("interloper.registry.base.MANIFESTS", _declaring(_entry("alpha", "A"))):
            assert len(registry) == 1


class TestManifestSection:
    def test_constructing_a_registry_claims_its_section(self):
        manifests = _declaring()
        with mock.patch("interloper.registry.base.MANIFESTS", manifests):
            Registry("things")
            Registry()
        manifests.claim.assert_called_once_with("things")

    def test_lazy_load_on_first_lookup(self):
        registry: Registry[str] = Registry("things")
        manifests = _declaring(_entry("alpha", "A"))
        with mock.patch("interloper.registry.base.MANIFESTS", manifests):
            assert registry.get("alpha") == "A"
            assert registry.keys() == ("alpha",)
        manifests.entries.assert_called_with("things")

    def test_no_section_never_loads(self):
        registry: Registry[str] = Registry()
        manifests = _declaring()
        with mock.patch("interloper.registry.base.MANIFESTS", manifests):
            assert registry.get("anything") is None
        manifests.entries.assert_not_called()

    def test_explicit_registration_wins_over_loaded(self):
        registry: Registry[str] = Registry("things")
        registry.register("alpha", "explicit")
        with mock.patch("interloper.registry.base.MANIFESTS", _declaring(_entry("alpha", "loaded"))):
            assert registry.get("alpha") == "explicit"

    def test_an_entry_shadowed_by_an_earlier_one_is_skipped_with_a_warning(self, caplog):
        registry: Registry[str] = Registry("things")
        declared = _declaring(_entry("alpha", "first", package="acme"), _entry("alpha", "second", package="other"))
        with mock.patch("interloper.registry.base.MANIFESTS", declared):
            assert registry.items() == (("alpha", "first"),)
        assert "Entry 'alpha' (other.things:alpha) of package 'other' is shadowed in section 'things'" in caplog.text

    def test_adopt_transforms_loaded_entries(self):
        registry: Registry[str] = Registry("things", adopt=lambda name, entry: (f"{name}!", entry.upper()))
        with mock.patch("interloper.registry.base.MANIFESTS", _declaring(_entry("alpha", "a"))):
            assert registry.get("alpha!") == "A"

    def test_an_entry_failing_to_adopt_is_skipped_and_recorded(self):
        def adopt(name: str, entry: object) -> tuple[str, str]:
            if name == "broken":
                raise TypeError(f"bad entry: {name}")
            return name, str(entry)

        registry: Registry[str] = Registry("things", adopt=adopt)
        manifests = _declaring(_entry("broken", "b"), _entry("alpha", "a"))
        with mock.patch("interloper.registry.base.MANIFESTS", manifests):
            assert registry.keys() == ("alpha",)
        manifests.record.assert_called_once_with(
            ExtensionFailure("acme", "things", "broken", "failed to load: TypeError: bad entry: broken")
        )

    def test_an_entry_failing_to_import_is_skipped_and_recorded(self):
        broken = _entry("broken", None)
        broken.load.side_effect = ImportError("cannot import name 'Old' from 'interloper'")
        registry: Registry[str] = Registry("things")
        manifests = _declaring(broken, _entry("alpha", "A"))
        with mock.patch("interloper.registry.base.MANIFESTS", manifests):
            assert registry.get("alpha") == "A"
            assert registry.keys() == ("alpha",)
        (failure,) = [call.args[0] for call in manifests.record.call_args_list]
        assert (failure.package, failure.section, failure.entry) == ("acme", "things", "broken")
        assert "cannot import name 'Old'" in failure.error

    def test_a_lookup_of_an_entry_that_failed_names_its_error(self):
        broken = _entry("kubernetes", None)
        broken.load.side_effect = ImportError("No module named 'kubernetes'")
        registry: Registry[str] = Registry("things")
        with (
            mock.patch("interloper.registry.base.MANIFESTS", _declaring(broken)),
            pytest.raises(KeyError, match="failed to load: ImportError: No module named 'kubernetes'"),
        ):
            registry["kubernetes"]

    def test_a_lookup_by_name_loads_only_that_names_entries(self):
        unrelated = _entry("beta", "B")
        registry: Registry[str] = Registry("things")
        with mock.patch("interloper.registry.base.MANIFESTS", _declaring(_entry("alpha", "A"), unrelated)):
            assert registry.get("alpha") == "A"
            unrelated.load.assert_not_called()
            assert registry.keys() == ("alpha", "beta")
        unrelated.load.assert_called_once()

    def test_a_lookup_by_name_through_adopt_loads_everything(self):
        registry: Registry[str] = Registry("things", adopt=lambda name, entry: (f"{name}!", entry))
        with mock.patch("interloper.registry.base.MANIFESTS", _declaring(_entry("alpha", "a"))):
            assert registry.get("alpha!") == "a"

    def test_failed_read_retries_on_next_lookup(self):
        # A transient failure to read the manifests must not latch the registry empty forever.
        registry: Registry[str] = Registry("things")
        unreadable = _declaring()
        unreadable.entries.side_effect = RuntimeError("boom")
        with mock.patch("interloper.registry.base.MANIFESTS", unreadable), pytest.raises(RuntimeError, match="boom"):
            registry.get("alpha")
        with mock.patch("interloper.registry.base.MANIFESTS", _declaring(_entry("alpha", "A"))):
            assert registry.get("alpha") == "A"


class TestThreadSafety:
    def test_concurrent_first_lookups_never_see_a_partial_registry(self):
        # Regression: a reader arriving while another thread was mid-load used
        # to observe an empty registry (KeyError "'rows' is not registered
        # (available: )") because _loaded flipped before the entries landed.
        registry: Registry[str] = Registry("things")

        entry = _entry("alpha", None)
        entry.load.side_effect = lambda: time.sleep(0.05) or "A"

        results: dict[str, object] = {}

        def lookup(tid: str) -> None:
            try:
                results[tid] = registry["alpha"]
            except KeyError as e:
                results[tid] = e

        with mock.patch("interloper.registry.base.MANIFESTS", _declaring(entry)):
            first = threading.Thread(target=lookup, args=("first",))
            second = threading.Thread(target=lookup, args=("second",))
            first.start()
            time.sleep(0.01)  # let the first thread enter the load
            second.start()
            first.join()
            second.join()

        assert results == {"first": "A", "second": "A"}


class TestErrorMessage:
    def test_unknown_name_lists_available(self):
        registry: Registry[str] = Registry()
        registry.register("alpha", "A")
        with pytest.raises(KeyError, match=r"'missing' is not registered \(available: alpha\)"):
            registry["missing"]

    def test_unknown_name_names_the_section(self):
        registry: Registry[str] = Registry("things")
        with (
            mock.patch("interloper.registry.base.MANIFESTS", _declaring(_entry("alpha", "A"))),
            pytest.raises(KeyError, match=r"'missing' is not registered in manifest section 'things'"),
        ):
            registry["missing"]

    def test_empty_section_hints_at_missing_package(self):
        registry: Registry[str] = Registry("things")
        with (
            mock.patch("interloper.registry.base.MANIFESTS", _declaring()),
            pytest.raises(KeyError, match=r"no entries discovered: is the package declaring it installed\?"),
        ):
            registry["rows"]
