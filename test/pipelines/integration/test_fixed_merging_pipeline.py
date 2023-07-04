# SPDX-FileCopyrightText: 2022-present deepset GmbH <info@deepset.ai>
#
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from pprint import pprint

from canals.pipeline import Pipeline
from test.sample_components import AddFixedValue, Subtract

import logging

logging.basicConfig(level=logging.DEBUG)


def test_pipeline(tmp_path):
    add_two = AddFixedValue(add=2)
    add_one = AddFixedValue(add=1)
    diff = Subtract()

    pipeline = Pipeline()
    pipeline.add_component("first_addition", add_two)
    pipeline.add_component("second_addition", add_two)
    pipeline.add_component("third_addition", add_two)
    pipeline.add_component("diff", diff)
    pipeline.add_component("fourth_addition", add_one)

    pipeline.connect("first_addition", "second_addition")
    pipeline.connect("second_addition", "diff.first_value")
    pipeline.connect("third_addition", "diff.second_value")
    pipeline.connect("diff", "fourth_addition.value")

    pipeline.draw(tmp_path / "fixed_merging_pipeline.png")

    results = pipeline.run(
        {
            "first_addition": add_two.input(value=1),
            "third_addition": add_two.input(value=1),
        }
    )
    pprint(results)

    assert results == {"fourth_addition": add_one.output(value=3)}


if __name__ == "__main__":
    test_pipeline(Path(__file__).parent)
