# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
# pylint: disable=missing-function-docstring

import logging
import pytest
from canals import Pipeline, marshal_pipelines, unmarshal_pipelines
from sample_components import AddFixedValue, Double

logging.basicConfig(level=logging.DEBUG)


def test_marshal():
    add_1 = AddFixedValue(add=200)
    add_2 = AddFixedValue()

    pipeline_1 = Pipeline(metadata={"type": "test pipeline", "author": "me"})
    pipeline_1.add_component("first_addition", add_1)
    pipeline_1.add_component("double", Double())
    pipeline_1.add_component("second_addition", add_2)
    pipeline_1.add_component("third_addition", add_1)

    pipeline_1.connect("first_addition.result", "double.value")
    pipeline_1.connect("double.value", "second_addition.value")
    pipeline_1.connect("second_addition.result", "third_addition.value")

    pipeline_2 = Pipeline(metadata={"type": "another test pipeline", "author": "you"})
    pipeline_2.add_component("first_addition", add_1)
    pipeline_2.add_component("double", Double())
    pipeline_2.add_component("second_addition", add_2)

    pipeline_2.connect("first_addition.result", "double.value")
    pipeline_2.connect("double.value", "second_addition.value")

    assert marshal_pipelines(pipelines={"pipe1": pipeline_1, "pipe2": pipeline_2}) == {
        "pipelines": {
            "pipe1": {
                "metadata": {"type": "test pipeline", "author": "me"},
                "max_loops_allowed": 100,
                "components": {
                    "first_addition": {
                        "type": "AddFixedValue",
                        "init_parameters": {"add": 200},
                    },
                    "double": {"type": "Double", "init_parameters": {}},
                    "second_addition": {
                        "type": "AddFixedValue",
                        "init_parameters": {},
                    },
                    "third_addition": {"refer_to": "pipe1.first_addition"},
                },
                "connections": [
                    ("first_addition", "double", "result/value"),
                    ("double", "second_addition", "value/value"),
                    ("second_addition", "third_addition", "result/value"),
                ],
            },
            "pipe2": {
                "metadata": {"type": "another test pipeline", "author": "you"},
                "max_loops_allowed": 100,
                "components": {
                    "first_addition": {"refer_to": "pipe1.first_addition"},
                    "double": {"type": "Double", "init_parameters": {}},
                    "second_addition": {"refer_to": "pipe1.second_addition"},
                },
                "connections": [
                    ("first_addition", "double", "result/value"),
                    ("double", "second_addition", "value/value"),
                ],
            },
        },
    }


def test_unmarshal():
    pipelines = unmarshal_pipelines(
        {
            "pipelines": {
                "pipe1": {
                    "metadata": {"type": "test pipeline", "author": "me"},
                    "max_loops_allowed": 100,
                    "components": {
                        "first_addition": {
                            "type": "AddFixedValue",
                            "init_parameters": {"add": 300},
                        },
                        "double": {"type": "Double"},
                        "second_addition": {
                            "type": "AddFixedValue",
                            "init_parameters": {},
                        },
                        "third_addition": {"refer_to": "pipe1.first_addition"},
                    },
                    "connections": [
                        ("first_addition", "double", "result/value"),
                        ("double", "second_addition", "value/value"),
                        ("second_addition", "third_addition", "result/value"),
                    ],
                },
                "pipe2": {
                    "metadata": {"type": "another test pipeline", "author": "you"},
                    "max_loops_allowed": 100,
                    "components": {
                        "first_addition": {"refer_to": "pipe1.first_addition"},
                        "double": {"type": "Double"},
                        "second_addition": {"refer_to": "pipe1.second_addition"},
                    },
                    "connections": [
                        ("first_addition", "double", "result/value"),
                        ("double", "second_addition", "value/value"),
                    ],
                },
            },
            "dependencies": ["test", "canals"],
        }
    )

    pipe1 = pipelines["pipe1"]
    assert pipe1.metadata == {"type": "test pipeline", "author": "me"}

    first_addition = pipe1.get_component("first_addition")
    assert isinstance(first_addition, AddFixedValue)
    assert pipe1.graph.nodes["first_addition"]["instance"].add == 300

    second_addition = pipe1.get_component("second_addition")
    assert isinstance(second_addition, AddFixedValue)
    assert pipe1.graph.nodes["second_addition"]["instance"].add == 1
    assert second_addition != first_addition

    third_addition = pipe1.get_component("third_addition")
    assert isinstance(third_addition, AddFixedValue)
    assert pipe1.graph.nodes["third_addition"]["instance"].add == 300
    assert third_addition == first_addition

    double = pipe1.get_component("double")
    assert isinstance(double, Double)

    assert list(pipe1.graph.edges) == [
        ("first_addition", "double", "result/value"),
        ("double", "second_addition", "value/value"),
        ("second_addition", "third_addition", "result/value"),
    ]

    pipe2 = pipelines["pipe2"]
    assert pipe2.metadata == {"type": "another test pipeline", "author": "you"}

    first_addition_2 = pipe2.get_component("first_addition")
    assert isinstance(first_addition_2, AddFixedValue)
    assert pipe2.graph.nodes["first_addition"]["instance"].add == 300
    assert first_addition_2 == first_addition

    second_addition_2 = pipe2.get_component("second_addition")
    assert isinstance(second_addition_2, AddFixedValue)
    assert pipe2.graph.nodes["second_addition"]["instance"].add == 1
    assert second_addition_2 != first_addition_2
    assert second_addition_2 == second_addition

    with pytest.raises(ValueError):
        pipe2.get_component("third_addition")

    double_2 = pipe2.get_component("double")
    assert isinstance(double_2, Double)
    assert double_2 != double

    assert list(pipe2.graph.edges) == [
        ("first_addition", "double", "result/value"),
        ("double", "second_addition", "value/value"),
    ]
