# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from typing import *
from pathlib import Path
from pprint import pprint

from canals.pipeline import Pipeline
from sample_components import Accumulate, AddFixedValue, Threshold, Sum, FirstIntSelector

import logging

logging.basicConfig(level=logging.DEBUG)


def test_pipeline(tmp_path):
    accumulator = Accumulate()
    pipeline = Pipeline(max_loops_allowed=10)
    pipeline.add_component("merge", FirstIntSelector())
    pipeline.add_component("sum", Sum())
    pipeline.add_component("below_10", Threshold(threshold=10))
    pipeline.add_component("add_one", AddFixedValue(add=1))
    pipeline.add_component("counter", accumulator)
    pipeline.add_component("add_two", AddFixedValue(add=2))

    pipeline.connect("merge", "below_10.value")
    pipeline.connect("below_10.below", "add_one.value")
    pipeline.connect("add_one.result", "counter.value")
    pipeline.connect("counter.value", "merge.input")
    pipeline.connect("below_10.above", "add_two.value")
    pipeline.connect("add_two.result", "sum.values")

    pipeline.draw(tmp_path / "looping_and_merge_pipeline.png")

    results = pipeline.run(
        {"merge": {"input": 8}, "sum": {"values": 2}},
    )
    pprint(results)
    print("accumulate: ", accumulator.state)

    assert results == {"sum": {"total": 23}}
    assert accumulator.state == 19


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
