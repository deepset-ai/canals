from typing import Optional

import pytest
import inspect

from canals.pipeline import Pipeline
from canals.errors import PipelineValidationError
from canals.pipeline.sockets import InputSocket, OutputSocket
from canals.pipeline.validation import _find_pipeline_inputs, _find_pipeline_outputs
from sample_components import Double, AddFixedValue, Sum, Parity


def test_find_pipeline_input_no_input():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")
    pipe.connect("comp2", "comp1")

    assert _find_pipeline_inputs(pipe.graph) == {"comp1": [], "comp2": []}


def test_find_pipeline_input_one_input():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")

    assert _find_pipeline_inputs(pipe.graph) == {
        "comp1": [InputSocket(name="value", type=int, is_optional=False)],
        "comp2": [],
    }


def test_find_pipeline_input_two_inputs_same_component():
    pipe = Pipeline()
    pipe.add_component("comp1", AddFixedValue())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")

    assert _find_pipeline_inputs(pipe.graph) == {
        "comp1": [
            InputSocket(name="value", type=int, is_optional=False),
            InputSocket(name="add", type=Optional[int], is_optional=True),
        ],
        "comp2": [],
    }


def test_find_pipeline_input_some_inputs_different_components():
    pipe = Pipeline()
    pipe.add_component("comp1", AddFixedValue())
    pipe.add_component("comp2", Double())
    pipe.add_component("comp3", AddFixedValue())
    pipe.connect("comp1.result", "comp3.value")
    pipe.connect("comp2.value", "comp3.add")

    assert _find_pipeline_inputs(pipe.graph) == {
        "comp1": [
            InputSocket(name="value", type=int, is_optional=False),
            InputSocket(name="add", type=Optional[int], is_optional=True),
        ],
        "comp2": [InputSocket(name="value", type=int, is_optional=False)],
        "comp3": [],
    }


def test_find_pipeline_variable_input_nodes_in_the_pipeline():
    pipe = Pipeline()
    pipe.add_component("comp1", AddFixedValue())
    pipe.add_component("comp2", Double())
    pipe.add_component("comp3", Sum(inputs=["in_1", "in_2"]))

    assert _find_pipeline_inputs(pipe.graph) == {
        "comp1": [
            InputSocket(name="value", type=int, is_optional=False),
            InputSocket(name="add", type=Optional[int], is_optional=True),
        ],
        "comp2": [InputSocket(name="value", type=int, is_optional=False)],
        "comp3": [
            InputSocket(name="in_1", type=Optional[int], is_optional=True),
            InputSocket(name="in_2", type=Optional[int], is_optional=True),
        ],
    }


def test_find_pipeline_output_no_output():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")
    pipe.connect("comp2", "comp1")

    assert _find_pipeline_outputs(pipe.graph) == {}


def test_find_pipeline_output_one_output():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")

    assert _find_pipeline_outputs(pipe.graph) == {"comp2": [OutputSocket(name="value", type=int)]}


def test_find_pipeline_some_outputs_same_component():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Parity())
    pipe.connect("comp1", "comp2")

    assert _find_pipeline_outputs(pipe.graph) == {
        "comp2": [OutputSocket(name="even", type=int), OutputSocket(name="odd", type=int)]
    }


def test_find_pipeline_some_outputs_different_components():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Parity())
    pipe.add_component("comp3", Double())
    pipe.connect("comp1", "comp2")
    pipe.connect("comp1", "comp3")

    assert _find_pipeline_outputs(pipe.graph) == {
        "comp2": [OutputSocket(name="even", type=int), OutputSocket(name="odd", type=int)],
        "comp3": [
            OutputSocket(name="value", type=int),
        ],
    }


def test_validate_pipeline_input_pipeline_with_no_inputs():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")
    pipe.connect("comp2", "comp1")
    with pytest.raises(PipelineValidationError, match="This pipeline has no inputs."):
        pipe.run({})


def test_validate_pipeline_input_unknown_component():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")
    with pytest.raises(ValueError, match="Pipeline received data for unknown component\(s\): test_component"):
        pipe.run({"test_component": {"value": 1}})


def test_validate_pipeline_input_all_necessary_input_is_present():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")
    with pytest.raises(ValueError, match="Missing input: comp1.value"):
        pipe.run({})


def test_validate_pipeline_input_all_necessary_input_is_present_considering_defaults():
    pipe = Pipeline()
    pipe.add_component("comp1", AddFixedValue())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")
    pipe.run({"comp1": {"value": 1}})
    pipe.run({"comp1": {"value": 1, "add": 2}})
    with pytest.raises(ValueError, match="Missing input: comp1.value"):
        pipe.run({"comp1": {"add": 3}})


def test_validate_pipeline_input_only_expected_input_is_present():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")
    with pytest.raises(ValueError, match="The input value of comp2 is already sent by node comp1"):
        pipe.run({"comp1": {"value": 1}, "comp2": {"value": 2}})


def test_validate_pipeline_input_only_expected_input_is_present_including_unknown_names():
    pipe = Pipeline()
    pipe.add_component("comp1", Double())
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")

    with pytest.raises(ValueError, match="Component comp1 is not expecting any input value called add"):
        pipe.run({"comp1": {"value": 1, "add": 2}})


def test_validate_pipeline_input_only_expected_input_is_present_and_defaults_dont_interfere():
    pipe = Pipeline()
    pipe.add_component("comp1", AddFixedValue(add=10))
    pipe.add_component("comp2", Double())
    pipe.connect("comp1", "comp2")
    assert pipe.run({"comp1": {"value": 1, "add": 5}}) == {"comp2": {"value": 12}}
