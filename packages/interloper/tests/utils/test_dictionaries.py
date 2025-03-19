import pytest

from interloper.utils.dictionaries import (
    regex_replace_in_keys,
    remove_empty_values,
    replace_in_keys,
    replace_in_values,
)


@pytest.fixture
def dictionary():
    return {"1": {"2": {"3": "4"}}}


class TestReplaceInKeys:
    def test_replace_in_keys(self, dictionary):
        new_dict = replace_in_keys(dictionary, "3", "5")

        assert new_dict == {"1": {"2": {"5": "4"}}}


class TestRegexReplaceInKeys:
    def test_regex_replace_in_keys(self, dictionary):
        new_dict = regex_replace_in_keys(dictionary, r"\d", "9")

        assert new_dict == {"9": {"9": {"9": "4"}}}


class TestReplaceInValues:
    def test_replace_value(self, dictionary):
        new_dict = replace_in_values(dictionary, "4", "5")

        assert new_dict == {"1": {"2": {"3": "5"}}}

    def test_replace_empty_dict(self):
        dictionary = {"1": {"2": {"3": {}}}}
        new_dict = replace_in_values(dictionary, {}, None)

        assert new_dict == {"1": {"2": {"3": None}}}

    def test_replace_empty_dict_in_list(self):
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
    def test_remove_empty_values(self):
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
