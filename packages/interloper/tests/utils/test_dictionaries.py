"""This module contains tests for the dictionary utility functions."""
import pytest

from interloper.utils.dictionaries import (
    regex_replace_in_keys,
    remove_empty_values,
    replace_in_keys,
    replace_in_values,
)


@pytest.fixture
def dictionary():
    """Return a sample dictionary for testing."""
    return {"1": {"2": {"3": "4"}}}


class TestReplaceInKeys:
    """Test the replace_in_keys function."""

    def test_replace_in_keys(self, dictionary):
        """Test replacing a string in the keys of a dictionary."""
        new_dict = replace_in_keys(dictionary, "3", "5")

        assert new_dict == {"1": {"2": {"5": "4"}}}


class TestRegexReplaceInKeys:
    """Test the regex_replace_in_keys function."""

    def test_regex_replace_in_keys(self, dictionary):
        """Test replacing a pattern in the keys of a dictionary."""
        new_dict = regex_replace_in_keys(dictionary, r"\d", "9")

        assert new_dict == {"9": {"9": {"9": "4"}}}


class TestReplaceInValues:
    """Test the replace_in_values function."""

    def test_replace_value(self, dictionary):
        """Test replacing a value in a dictionary."""
        new_dict = replace_in_values(dictionary, "4", "5")

        assert new_dict == {"1": {"2": {"3": "5"}}}

    def test_replace_empty_dict(self):
        """Test replacing an empty dictionary."""
        dictionary = {"1": {"2": {"3": {}}}}
        new_dict = replace_in_values(dictionary, {}, None)

        assert new_dict == {"1": {"2": {"3": None}}}

    def test_replace_empty_dict_in_list(self):
        """Test replacing an empty dictionary in a list."""
        dictionary = {
            "1": {
                "2": {
                    "3": [
                        {"4": "5"},
                        {"6": {}},
                    ],
                    "4": ["7", "8"],
                },
            }
        }
        new_dict = replace_in_values(dictionary, {}, None)

        assert new_dict == {
            "1": {
                "2": {
                    "3": [
                        {"4": "5"},
                        {"6": None},
                    ],
                    "4": ["7", "8"],
                },
            }
        }


class TestRemoveEmptyValues:
    """Test the remove_empty_values function."""

    def test_remove_empty_values(self):
        """Test removing empty values from a dictionary."""
        dictionary = {
            "1": "2",
            "3": None,
            "4": {"1": "2", "3": None},
        }

        new_dict = remove_empty_values(dictionary)

        assert new_dict == {
            "1": "2",
            "4": {"1": "2", "3": None},
        }

    def test_remove_empty_values_recursive(self):
        """Test removing empty values from a dictionary recursively."""
        dictionary = {
            "1": "2",
            "3": None,
            "4": {"1": "2", "3": None},
        }

        new_dict = remove_empty_values(dictionary, recursive=True)

        assert new_dict == {
            "1": "2",
            "4": {"1": "2"},
        }
