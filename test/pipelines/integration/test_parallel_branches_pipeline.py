# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from pprint import pprint

from canals.pipeline import Pipeline
from test.sample_components import AddFixedValue, Repeat, Double

import logging

logging.basicConfig(level=logging.DEBUG)


def test_pipeline(tmp_path):
    add_one = AddFixedValue(add=1)
    add_ten = AddFixedValue(add=10)
    double = Double()

    pipeline = Pipeline()
    pipeline.add_component("add_one", add_one)
    pipeline.add_component("repeat", Repeat(outputs=["first", "second"]))
    pipeline.add_component("add_ten", add_ten)
    pipeline.add_component("double", double)
    pipeline.add_component("add_three", AddFixedValue(add=3))
    pipeline.add_component("add_one_again", add_one)

    pipeline.connect("add_one.value", "repeat.value")
    pipeline.connect("repeat.first", "add_ten.value")
    pipeline.connect("repeat.second", "double")
    pipeline.connect("repeat.second", "add_three.value")
    pipeline.connect("add_three", "add_one_again")

    pipeline.draw(tmp_path / "parallel_branches_pipeline.png")

    results = pipeline.run({"add_one": AddFixedValue().input(value=1)})
    pprint(results)

    assert results == {
        "add_one_again": add_one.output(value=6),
        "add_ten": add_ten.output(value=12),
        "double": double.output(value=4),
    }


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
