# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Dict

import pytest

from canals.errors import DeserializationError

from sample_components import MergeLoop


def test_to_dict():
    component = MergeLoop(expected_type=int)
    res = component.to_dict()
    assert res == {
        "type": "MergeLoop",
        "init_parameters": {"expected_type": "builtins.int"},
    }


def test_to_dict_with_typing_class():
    component = MergeLoop(expected_type=Dict)
    res = component.to_dict()
    assert res == {
        "type": "MergeLoop",
        "init_parameters": {
            "expected_type": "typing.Dict",
        },
    }


def test_to_dict_with_custom_class():
    component = MergeLoop(expected_type=MergeLoop)
    res = component.to_dict()
    assert res == {
        "type": "MergeLoop",
        "init_parameters": {
            "expected_type": "sample_components.merge_loop.MergeLoop",
        },
    }


def test_from_dict():
    data = {
        "type": "MergeLoop",
        "init_parameters": {"expected_type": "builtins.int"},
    }
    component = MergeLoop.from_dict(data)
    assert component.expected_type == "builtins.int"


def test_from_dict_with_typing_class():
    data = {
        "type": "MergeLoop",
        "init_parameters": {
            "expected_type": "typing.Dict",
        },
    }
    component = MergeLoop.from_dict(data)
    assert component.expected_type == "typing.Dict"


def test_from_dict_with_custom_class():
    data = {
        "type": "MergeLoop",
        "init_parameters": {
            "expected_type": "sample_components.merge_loop.MergeLoop",
        },
    }
    component = MergeLoop.from_dict(data)
    assert component.expected_type == "sample_components.merge_loop.MergeLoop"


def test_from_dict_without_expected_type():
    data = {
        "type": "MergeLoop",
        "init_parameters": {},
    }
    with pytest.raises(DeserializationError) as exc:
        MergeLoop.from_dict(data)

    exc.match("Missing 'expected_type' field in 'init_parameters'")


def test_merge_first():
    component = MergeLoop(expected_type=int)
    results = component.run(values=[5, None])
    assert results == {"value": 5}


def test_merge_second():
    component = MergeLoop(expected_type=int)
    results = component.run(values=[None, 5])
    assert results == {"value": 5}


def test_merge_nones():
    component = MergeLoop(expected_type=int)
    results = component.run(values=[None, None])
    assert results == {"value": None}


def test_merge_one():
    component = MergeLoop(expected_type=int)
    results = component.run(values=[1])
    assert results == {"value": 1}


def test_merge_one_none():
    component = MergeLoop(expected_type=int)
    results = component.run(values=[])
    assert results == {"value": None}
