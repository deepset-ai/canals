# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Optional, overload
import sys
from pathlib import Path
from pprint import pprint

import pytest

from canals import component
from canals.pipeline import Pipeline
from sample_components import AddFixedValue

import logging

logging.basicConfig(level=logging.DEBUG)


@pytest.mark.skipif(sys.version_info >= (3, 11), reason="This syntax only works starting from Python 3.11")
def test_pipeline_overload_syntax(tmp_path):
    @component
    class Decision:
        def __init__(self, target: int = 5):
            self.target = target

        @overload
        def run(self, initial_value: int):
            ...

        @overload
        def run(self, increased: int, decreased: int):
            ...

        @component.output_types(result=int, to_increase=int, to_decrease=int)
        def run(
            self, initial_value: Optional[int] = None, increased: Optional[int] = None, decreased: Optional[int] = None
        ):
            if initial_value is not None:
                increased = initial_value
                decreased = initial_value

            if decreased == self.target or increased == self.target:
                return {"result": self.target}

            return {"to_increase": increased, "to_decrease": decreased}

    pipeline = Pipeline(max_loops_allowed=10)
    pipeline.add_component("decision", Decision())
    pipeline.add_component("increase", AddFixedValue(add=0.5))
    pipeline.add_component("increase2", AddFixedValue(add=0.5))
    pipeline.add_component("decrease", AddFixedValue(add=-1))
    pipeline.connect("decision.to_decrease", "decrease.value")
    pipeline.connect("decrease", "decision.decreased")
    pipeline.connect("decision.to_increase", "increase.value")
    pipeline.connect("increase", "increase2.value")
    pipeline.connect("increase2", "decision.increased")

    pipeline.draw(tmp_path / "distinct_loops_pipeline.png")

    results = pipeline.run({"decision": {"initial_value": 0}})
    pprint(results)

    assert results["decision"]["result"] == 5


def test_pipeline_repeated_set_input_types(tmp_path):
    @component
    class Decision:
        def __init__(self, target: int = 5):
            self.target = target
            component.set_input_types(self, initial_value=int)
            component.set_input_types(self, increased=int, decreased=int)

        @component.output_types(result=int, to_increase=int, to_decrease=int)
        def run(
            self, initial_value: Optional[int] = None, increased: Optional[int] = None, decreased: Optional[int] = None
        ):
            if initial_value is not None:
                increased = initial_value
                decreased = initial_value

            if decreased == self.target or increased == self.target:
                return {"result": self.target}

            return {"to_increase": increased, "to_decrease": decreased}

    pipeline = Pipeline(max_loops_allowed=10)
    pipeline.add_component("decision", Decision())
    pipeline.add_component("increase", AddFixedValue(add=0.5))
    pipeline.add_component("increase2", AddFixedValue(add=0.5))
    pipeline.add_component("decrease", AddFixedValue(add=-1))
    pipeline.connect("decision.to_decrease", "decrease.value")
    pipeline.connect("decrease", "decision.decreased")
    pipeline.connect("decision.to_increase", "increase.value")
    pipeline.connect("increase", "increase2.value")
    pipeline.connect("increase2", "decision.increased")

    pipeline.draw(tmp_path / "distinct_loops_pipeline.png")

    results = pipeline.run({"decision": {"initial_value": 0}})
    pprint(results)

    assert results["decision"]["result"] == 5


if __name__ == "__main__":
    test_pipeline_overload_syntax(Path(__file__).parent)
    test_pipeline_repeated_set_input_types(Path(__file__).parent)
