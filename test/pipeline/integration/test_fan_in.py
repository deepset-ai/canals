# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import Type, List
from pathlib import Path
from pprint import pprint

from canals.pipeline import Pipeline
from canals import component
from canals.component.types import Variadic
from sample_components import AddFixedValue, Double

import logging

logging.basicConfig(level=logging.DEBUG)


@component
class Joiner:
    def __init__(self, inputs_type: Type):
        component.set_input_types(self, input=Variadic[inputs_type])
        component.set_output_types(self, output=List[inputs_type])

    def run(self, input):
        return {"output": input}


def test_pipeline(tmp_path):
    pipeline = Pipeline()
    pipeline.add_component("inc", AddFixedValue(add=1))
    pipeline.add_component("dec", AddFixedValue(add=-1))
    pipeline.add_component("joiner", Joiner(int))

    pipeline.connect("inc", "joiner")
    pipeline.connect("dec", "joiner")

    pipeline.draw(tmp_path / "linear_pipeline.png")

    results = pipeline.run({"inc": {"value": 4}, "dec": {"value": 11}})
    pprint(results)

    assert results == {"joiner": {"output": [5, 10]}}


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
