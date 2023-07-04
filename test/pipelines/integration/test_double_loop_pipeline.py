# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import *
from pathlib import Path
from pprint import pprint

from canals.pipeline import Pipeline
from test.sample_components import Accumulate, AddFixedValue, Threshold, MergeLoop

import logging

logging.basicConfig(level=logging.DEBUG)


def test_pipeline(tmp_path):
    accumulator = Accumulate()
    add_one = AddFixedValue(add=1)
    add_two = AddFixedValue(add=2)
    merge_loop = MergeLoop(expected_type=int, inputs=["in_1", "in_2", "in_3"])

    pipeline = Pipeline(max_loops_allowed=10)
    pipeline.add_component("add_one", add_one)
    pipeline.add_component("merge", merge_loop)
    pipeline.add_component("below_10", Threshold(threshold=10))
    pipeline.add_component("below_5", Threshold(threshold=5))
    pipeline.add_component("add_three", AddFixedValue(add=3))
    pipeline.add_component("accumulator", accumulator)
    pipeline.add_component("add_two", add_two)

    pipeline.connect("add_one", "merge.in_1")
    pipeline.connect("merge", "below_10")
    pipeline.connect("below_10.below", "accumulator")
    pipeline.connect("accumulator", "below_5")
    pipeline.connect("below_5.above", "add_three.value")
    pipeline.connect("below_5.below", "merge.in_2")
    pipeline.connect("add_three", "merge.in_3")
    pipeline.connect("below_10.above", "add_two.value")

    pipeline.draw(tmp_path / "double_loop_pipeline.png")

    results = pipeline.run({"add_one": add_one.input(value=3)})
    pprint(results)
    print("accumulator: ", accumulator.state)

    assert results == {"add_two": add_two.output(value=13)}
    assert accumulator.state == 8


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
